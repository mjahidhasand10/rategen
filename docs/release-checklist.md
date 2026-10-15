# Release checklist

Release the core module first. Sub-modules depend on a published core
version.

## Before tagging

- [ ] CI is green on `main` for every supported Go version, including Redis
      integration tests.
- [ ] Locally, with the latest Go patch release and Redis running
      (`scripts/redis-up.sh`), every module passes:
      ```sh
      gofmt -l .                       # prints nothing
      for m in . redis prometheus benchmarks; do (cd $m &&
        go vet ./... &&
        go test -race -count=1 ./... &&
        go run honnef.co/go/tools/cmd/staticcheck@2026.1 ./...); done
      for m in . redis prometheus; do (cd $m && go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...); done
      go test -run=NONE -bench=. -benchmem ./...
      ```
- [ ] Dependency licenses are still MIT, BSD or Apache-2.0:
      `go run github.com/google/go-licenses/v2@v2.0.1 report ./...` in
      `redis/` and `prometheus/` (with `GOWORK=off`).
- [ ] The API review is done: `go doc -all` for each package. Every exported
      identifier is documented, and nothing is exported by accident.
- [ ] `docs/` and examples match the API. `go build ./examples/...` passes in
      the core and `redis` modules.
- [ ] If the Redis script or key layout changed, the release notes say that
      limits reset once on upgrade.
- [ ] `docs/benchmarks.md` was regenerated if performance claims changed.
      Never edit numbers by hand.
- [ ] The release notes list breaking changes with migration steps.

## Tagging

1. Tag the core: `git tag v0.X.Y && git push origin v0.X.Y`.
2. In `redis/go.mod` and `prometheus/go.mod`:
   - set `require github.com/mjahidhasand10/rategen v0.X.Y`;
   - **delete the `replace github.com/mjahidhasand10/rategen => ../` line**,
     which is for development only and would break `go get` for users;
   - run `GOWORK=off go mod tidy`, then commit.
3. Tag each sub-module: `git tag redis/v0.X.Y prometheus/v0.X.Y` and push
   the tags.
4. Restore the `replace` lines on `main` after the release commit, or keep
   developing through `go.work` (which needs no `replace` once the core is
   published).
5. Check that `go get github.com/mjahidhasand10/rategen/redis@v0.X.Y` works in
   an empty module with `GOWORK=off` and `GOFLAGS=-mod=mod`.
6. Publish a GitHub release with the notes.

The `benchmarks` module is never tagged. It exists for comparisons only.
