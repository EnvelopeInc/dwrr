# Releasing

This repository publishes the Go module `github.com/version2beta/dwrr`.

## Before release

- Start from a clean, up-to-date `main` checkout with the intended changes merged.
- Confirm CI is green on `main`; locally, run `go test -race ./...` and `go vet ./...`.
- Choose a semantic version. While the module is `v0.x`, its API is unstable; `v1.0.0` denotes the stable API. For a future major version `v2` or later, update the module path in `go.mod` and imports to include the major suffix, for example `github.com/version2beta/dwrr/v2`, before tagging.

## Publish

1. Create and push an annotated version tag, for example:

   ```sh
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

   Use the intended `vX.Y.Z` version, without a `v` prefix in `go.mod`. Pushing a `v*` tag runs CI. Never move or retag a published version; publish a new version instead.
2. Create a GitHub Release for that exact tag and include concise release notes.
3. Verify the exact version resolves from the Go module proxy and that a consumer can fetch it:

   ```sh
   GOPROXY=https://proxy.golang.org go list -m github.com/version2beta/dwrr@v0.1.0
   tmp="$(mktemp -d)"
   cd "$tmp"
   go mod init example.com/dwrr-consumer
   GOPROXY=https://proxy.golang.org go get github.com/version2beta/dwrr@v0.1.0
   ```

   Substitute the release version. For v2+, use the `/v2` module path. Confirm the resolved module and version match the release; `go get` may be run in a temporary consumer module to avoid changing this repository's module state.
4. Check that the version appears on [pkg.go.dev](https://pkg.go.dev/github.com/version2beta/dwrr). The Go proxy and pkg.go.dev index new versions asynchronously; request indexing on pkg.go.dev if needed.
