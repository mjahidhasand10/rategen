module github.com/mjahidhasand10/rategen/redis

go 1.26.0

toolchain go1.26.9

require (
	github.com/mjahidhasand10/rategen v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

// Development only: resolves the core module from this repository. Remove
// before tagging a release (see docs/release-checklist.md).
replace github.com/mjahidhasand10/rategen => ../
