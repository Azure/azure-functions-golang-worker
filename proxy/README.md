# Proxy

The proxy handles Azure Functions placeholder mode for the Go worker in flex consumption. Since the user's compiled Go binary doesn't exist during placeholder mode (it hasn't been deployed yet), the proxy stands in — handling the gRPC protocol with the host until the real app arrives.

## How It Works

```
Container Start
    │
    ├─ App binary exists? ──YES──► syscall.Exec(app) ──► App talks directly to host (zero overhead)
    │
    ├─ Placeholder mode? ──NO───► log.Fatal (misconfiguration)
    │
    └─ Placeholder mode ──YES──► Run proxy protocol
         │
         ├─ Connect to host, respond to WorkerInit, heartbeats, metadata (empty)
         │
         ├─ Host sends FunctionEnvironmentReloadRequest (specialization)
         │    ├─ Apply FERR env vars to proxy process (os.Setenv)
         │    ├─ Spawn child: /home/site/wwwroot/app → connects to proxy's local gRPC server
         │    ├─ Replay WorkerInitRequest to child, capture capabilities
         │    └─ Send FunctionEnvironmentReloadResponse with child's real capabilities
         │
         └─ Steady state: transparent gRPC bridge (host ↔ proxy ↔ child)
```

After specialization, the proxy is a transparent pass-through. Every message from the host goes to the child; every response from the child goes to the host.

## Version and release assets

The proxy version is independent of the Go worker module version. The reviewed
value in `proxy/version.txt` selects the proxy to ship. For example, Go worker
`v1.0.3` can include proxy `1.0.1`.

Release builds embed the proxy version and its source commit using Go linker
flags. The same version is sent to the Functions host during placeholder
initialization, printed in the startup log, and available without host
configuration or app startup:

```bash
./proxy-linux-amd64 --version
# proxy 1.0.1 (commit <source commit SHA>)
```

Local builds without linker flags report `dev` and an `unknown` source commit.
`--version` is handled before checking for or executing the user's app.

The manually queued `eng/ci/release.yml` still requires a successful official
build of the release commit. It now also publishes GitHub Releases and assets:

1. If `proxy/v<version>` does not exist, test and build a static Linux amd64
   proxy from the selected official build commit. Verify its embedded identity
   and generate `proxy-linux-amd64.sha256`.
2. If the proxy tag exists, download the original binary and checksum from its
   published proxy release. Verify the checksum and embedded version/source.
   Do not build current proxy source, even if it has changed. A version bump
   is the explicit approval to ship those changes.
3. Create and verify the root and submodule tags as before.
4. After consumer verification and the build job's artifact checks succeed,
   save the `proxy-release` pipeline artifact. A dependent publication job
   downloads it and publishes a new proxy tag and a **non-latest** release,
   such as `proxy/v1.0.1`, preserving the canonical binary and checksum.
   Creating this proxy tag and release is skipped when reusing a proxy.
5. Attach the exact proxy binary and checksum to a draft Go worker release
   with generated notes. Verify the uploaded bytes before publishing. Stable
   Go worker releases are marked latest. Prereleases are not.

The existing GitHub checkout service connection needs repository **Contents
read/write** permission for tags, releases and assets. The release helper reuses
its persisted Authorization header without printing it. No personal token is
stored in source or pipeline artifacts. API permission and network failures
stop the run rather than being treated as an absent release.

### Docker consumption

Every Go worker release carries the asset name used by `v0.3.0-preview`:
`proxy-linux-amd64`. A Docker build can use
`https://github.com/Azure/azure-functions-golang-worker/releases/latest/download/proxy-linux-amd64`.

Resolve the latest **Go worker release tag once**, then download both the
binary and `proxy-linux-amd64.sha256` using that versioned release URL.
Using `latest` separately for both downloads could cross a release publication.
Verify with `sha256sum --check proxy-linux-amd64.sha256`, set the binary's
executable bit, and run `./proxy-linux-amd64 --version` in the Docker build.
That output records the actual proxy version and source, which can differ from
the containing Go worker release version. A checksum detects corruption, not
publisher authenticity; downloads still rely on GitHub HTTPS and release access.

The Docker repository is updated separately. Rebuilding an old Docker commit
against `latest` can select a newer release.

### Failure recovery

The pipeline never moves an existing tag or replaces an existing asset.
Matching module tags and matching Go worker draft assets allow a retry to
continue without overwriting them. A completed release is verified without
republishing it or changing its latest status.

Retrying only the publication job uses the original `proxy-release` artifact.
If that attempt already published the proxy, its tag must match the prepared
source commit and its published assets must match the artifact byte for byte.
Only then can the job resume the same Go worker draft. It does not rebuild or
replace the proxy.

If a proxy tag exists without a published proxy release or either canonical
asset, the run **fails without rebuilding**. Preserve the original output from
the release agent or `proxy-release` pipeline artifact and reconcile the tag,
draft release, binary and checksum before retrying. Do not delete the tag to
force a different build under the same version. A GitHub request with an
ambiguous outcome must be reconciled before another write.

Module tags can already be public if consumer verification or release upload
fails. Failure does not roll back tags or releases. A draft Go worker release
is not marked latest until both attachments have been verified.

## Exec Bypass

On worker restart (after the app has been deployed), the proxy detects the binary at startup and replaces itself via `syscall.Exec`. The host sees the same PID, but the process is now the real app — zero proxy overhead.

## Child Lifecycle

If the child process dies, the proxy exits with the same exit code. The host detects the process exit and restarts it, triggering the exec bypass path.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FUNCTIONS_APP_BINARY_NAME` | `app` | Name of the app binary |
| `WEBSITE_PLACEHOLDER_MODE` | — | Must be `1` for the proxy to run |

## Files

- `main.go` — Proxy implementation (~380 lines)
- `main_test.go` — Unit tests for `appBinaryPath` and env override behavior
