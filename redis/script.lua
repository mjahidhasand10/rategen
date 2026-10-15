-- rategen: atomic rate-limit decisions for one or more checks.
--
-- KEYS[i]  state hash for check i
-- ARGV[1]  mode: "allow" (all-or-nothing) or "refund"
-- ARGV[2]  number of checks
-- then 7 values per check:
--   kind ("tb", "fw", "sw", "gc"), n, limit, window_us, interval_us,
--   emission_us, epoch (refunds only: the window the units were charged to)
--
-- Time comes from the server (TIME), in microseconds. This script writes
-- after calling TIME, which requires effects replication (the default since
-- Redis 5 and the only mode in Redis 7).
--
-- State fields mirror internal/algo.State: a, b, c (integers) and f (float).
--
-- Returns for "allow": {allowed_all, then per check: allowed, remaining,
-- retry_us, reset_us, epoch}. Returns {1} for "refund".

if redis.replicate_commands then redis.replicate_commands() end

local mode = ARGV[1]
local count = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local EPS = 1e-9

local function ceilu(x)
  if x <= 0 then return 0 end
  local r = math.ceil(x - 1e-3)
  if r < 1 then r = 1 end
  return r
end

-- The window containing now, but never earlier than the stored window: a
-- server clock that steps backwards (for example after failover) must not
-- roll a window back and reset its count. Mirrors algo.currentWindow.
local function wstart(w, stored)
  return math.max(now - (now % w), stored)
end

-- Time until a bucket holding `tokens` has `need`, verified against the
-- refill arithmetic and nudged up so a retry at exactly that time is
-- admitted. Mirrors algo.tokenWait.
local function token_wait(p, tokens, need)
  local d = ceilu((need - tokens) * p.interval)
  for _ = 1, 8 do
    if tokens + d / p.interval >= need - EPS then return d end
    d = d + 1
  end
  return d + ceilu(EPS * p.interval) + 1
end

-- Sliding-window estimate at time `at`, without modifying s.
local function sw_estimate(p, s, at)
  local w = p.window
  local start = math.max(at - (at % w), s.a)
  local b, c = s.b, s.c
  if s.a ~= start then
    if s.a == start - w then c = s.b else c = 0 end
    b = 0
  end
  local elapsed = math.max(0, at - start)
  return c * ((w - elapsed) / w) + b
end

local function load(key, p)
  local v = redis.call('HMGET', key, 'a', 'b', 'c', 'f')
  if not v[1] then
    if p.kind == 'tb' then
      return {a = now, b = 0, c = 0, f = p.limit}
    end
    return {a = 0, b = 0, c = 0, f = 0}
  end
  return {a = tonumber(v[1]), b = tonumber(v[2]) or 0, c = tonumber(v[3]) or 0, f = tonumber(v[4]) or 0}
end

local function copy(s)
  return {a = s.a, b = s.b, c = s.c, f = s.f}
end

local function refill(p, s)
  if now > s.a then
    s.f = math.min(p.limit, s.f + (now - s.a) / p.interval)
    s.a = now
  end
end

local function roll(p, s)
  local start = wstart(p.window, s.a)
  if s.a ~= start then
    if s.a == start - p.window then s.c = s.b else s.c = 0 end
    s.a = start
    s.b = 0
  end
end

-- decide returns allowed, remaining, retry, reset, epoch; s is updated in
-- place (callers pass a copy and keep it only when allowed).
local function decide(p, s, n)
  if p.kind == 'tb' then
    refill(p, s)
    local allowed, retry = false, 0
    if s.f >= n - EPS then
      s.f = math.max(0, s.f - n)
      allowed = true
    else
      retry = token_wait(p, s.f, n)
    end
    return allowed, math.floor(s.f + EPS), retry, ceilu((p.limit - s.f) * p.interval), 0
  elseif p.kind == 'fw' then
    local start = wstart(p.window, s.a)
    if s.a ~= start then s.a = start; s.b = 0 end
    local reset = start + p.window - math.max(now, start)
    if s.b + n <= p.limit then
      s.b = s.b + n
      return true, math.max(0, p.limit - s.b), 0, reset, s.a
    end
    -- The count can exceed a limit lowered under the same policy name.
    return false, math.max(0, p.limit - s.b), reset, reset, 0
  elseif p.kind == 'sw' then
    roll(p, s)
    local elapsed = math.max(0, now - s.a)
    local w = p.window
    local est = s.c * ((w - elapsed) / w) + s.b
    local allowed, retry, epoch = false, 0, 0
    if est + n <= p.limit + EPS then
      s.b = s.b + n
      est = est + n
      allowed = true
      epoch = s.a
    else
      local avail = p.limit - s.b - n
      if avail >= 0 and s.c > 0 then
        retry = math.max(1, ceilu(w * (1 - avail / s.c)) - elapsed)
      else
        local e2 = 0
        if s.b > 0 then e2 = math.max(0, w * (1 - (p.limit - n) / s.b)) end
        retry = w - elapsed + ceilu(e2)
      end
      -- Verify against the estimate a retry will see (float error in the
      -- weights). Mirrors algo.decideSlidingWindow.
      for _ = 1, 8 do
        if sw_estimate(p, s, now + retry) + n <= p.limit + EPS then break end
        retry = retry + 1
      end
    end
    local remaining = math.max(0, math.floor(p.limit - est + EPS))
    local reset = 0
    if s.b > 0 then reset = 2 * w - elapsed elseif s.c > 0 then reset = w - elapsed end
    return allowed, remaining, retry, reset, epoch
  else -- gc
    local tau = p.emission * p.limit
    local tat = math.max(s.a, now)
    local newtat = tat + n * p.emission
    local allow_at = newtat - tau
    if now >= allow_at then
      s.a = newtat
      return true, math.floor((tau - (newtat - now)) / p.emission), 0, newtat - now, 0
    end
    return false, math.max(0, math.floor((tau - (tat - now)) / p.emission)), allow_at - now, tat - now, 0
  end
end

-- Window refunds go to the window the units were charged to (epoch), and
-- are dropped if it no longer counts. Mirrors algo.Refund.
local function refund(p, s, n, epoch)
  if p.kind == 'tb' then
    refill(p, s)
    s.f = math.min(p.limit, s.f + n)
  elseif p.kind == 'fw' then
    if epoch ~= 0 and s.a == epoch and s.a == wstart(p.window, s.a) then
      s.b = math.max(0, s.b - n)
    end
  elseif p.kind == 'sw' then
    roll(p, s)
    if epoch == 0 then
      -- unknown window: drop
    elseif epoch == s.a then
      s.b = math.max(0, s.b - n)
    elseif epoch == s.a - p.window then
      s.c = math.max(0, s.c - n)
    end
  else
    s.a = math.max(now, s.a - n * p.emission)
  end
end

local function expires(p, s)
  if p.kind == 'tb' then
    local missing = p.limit - s.f
    if missing <= 0 then return s.a end
    -- Rounded up generously, like algo.ExpiresAt.
    return s.a + math.ceil(missing * p.interval) + 1
  elseif p.kind == 'fw' then
    return s.a + p.window
  elseif p.kind == 'sw' then
    if s.b > 0 then return s.a + 2 * p.window end
    return s.a + p.window
  end
  return s.a
end

local function save(key, p, s)
  local ttl = math.ceil((expires(p, s) - now) / 1000)
  if ttl < 1 then
    redis.call('DEL', key)
    return
  end
  redis.call('HSET', key,
    'a', string.format('%.0f', s.a),
    'b', string.format('%.0f', s.b),
    'c', string.format('%.0f', s.c),
    'f', string.format('%.17g', s.f))
  redis.call('PEXPIRE', key, ttl)
end

local params = {}
for i = 1, count do
  local o = 2 + (i - 1) * 7
  params[i] = {
    kind = ARGV[o + 1],
    n = tonumber(ARGV[o + 2]),
    limit = tonumber(ARGV[o + 3]),
    window = tonumber(ARGV[o + 4]),
    interval = tonumber(ARGV[o + 5]),
    emission = tonumber(ARGV[o + 6]),
    epoch = tonumber(ARGV[o + 7]),
  }
end

-- States are cached by key so that several checks on the same key within
-- one call see each other's effects.
local cache = {}
local function state(i)
  local k = KEYS[i]
  if cache[k] == nil then cache[k] = load(k, params[i]) end
  return cache[k]
end

if mode == 'refund' then
  for i = 1, count do
    local s = state(i)
    refund(params[i], s, params[i].n, params[i].epoch)
    save(KEYS[i], params[i], s)
  end
  return {1}
end

local out = {1}
local next_states = {}
for i = 1, count do
  local p = params[i]
  local s = copy(state(i))
  local allowed, remaining, retry, reset, epoch = decide(p, s, p.n)
  if allowed then
    cache[KEYS[i]] = s
    next_states[KEYS[i]] = i
  else
    out[1] = 0
  end
  out[#out + 1] = allowed and 1 or 0
  out[#out + 1] = remaining
  out[#out + 1] = retry
  out[#out + 1] = reset
  out[#out + 1] = epoch
end

if out[1] == 1 then
  for k, i in pairs(next_states) do
    save(k, params[i], cache[k])
  end
end
return out
