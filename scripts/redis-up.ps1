# Starts a disposable Redis for integration tests and sets RATEGEN_REDIS_ADDR
# for the current session. Usage: scripts/redis-up.ps1 [-Image redis:7.4-alpine]
param([string]$Image = "redis:7.4-alpine")
$name = "rategen-redis"
$running = docker ps --format "{{.Names}}" | Where-Object { $_ -eq $name }
if (-not $running) {
    docker rm -f $name 2>$null | Out-Null
    docker run -d --name $name -p 6379:6379 $Image | Out-Null
}
do { Start-Sleep -Milliseconds 200; docker exec $name redis-cli ping 2>$null | Out-Null } until ($LASTEXITCODE -eq 0)
$env:RATEGEN_REDIS_ADDR = "127.0.0.1:6379"
Write-Output "RATEGEN_REDIS_ADDR=$env:RATEGEN_REDIS_ADDR"
