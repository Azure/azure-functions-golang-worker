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
