module github.com/mjahidhasand10/rategen/benchmarks

go 1.26.0

toolchain go1.26.9

replace (
	github.com/mjahidhasand10/rategen => ../
	github.com/mjahidhasand10/rategen/redis => ../redis
)

require (
	github.com/didip/tollbooth/v8 v8.0.1
	github.com/go-redis/redis_rate/v10 v10.0.1
	github.com/mjahidhasand10/rategen v0.0.0-00010101000000-000000000000
	github.com/mjahidhasand10/rategen/redis v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.22.0
	github.com/ulule/limiter/v3 v3.11.2
	golang.org/x/time v0.15.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-pkgz/expirable-cache/v3 v3.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)
