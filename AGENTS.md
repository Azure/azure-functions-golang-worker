# AGENTS.md

## Project

Azure Functions Go Worker — a Go SDK and gRPC worker process for writing Azure Functions in Go.

- **Language:** Go 1.24+
- **Module:** `github.com/azure/azure-functions-golang-worker`

## Structure

- `sdk/` — Public SDK (bindings, app registration, fluent builders)
- `sdk/bindings/` — One file per trigger/binding type + co-located `_test.go` files
- `sdk/extensions/` — Deferred binding extensions (one subpackage per Azure service, e.g. `blob/`, `eventhub/`)
- `sdk/registry.go` — Converter registry (`RegisterConverter`/`GetConverter`) for deferred bindings
- `worker/` — gRPC worker process (dispatcher, converters, handlers)
- `proxy/` — Lightweight proxy for consumption/placeholder mode
- `samples/` — One directory per trigger type (httpTrigger, timerTrigger, etc.)
- `tests/consumption_tests/` — Python-based Docker integration tests

## Rules

- Follow existing patterns exactly. Every trigger type uses the same 3-layer structure (bindings type, common.go registration, app.go builder). Deferred bindings add a 4th layer via `sdk/extensions/`.
- For deferred bindings (where users receive Azure SDK clients instead of raw data), create an extension package under `sdk/extensions/<type>/` with an `init()` that calls `sdk.RegisterConverter`. Users import it with a blank import (`_ "...extensions/<type>"`).
- Use pointer receivers (`*T`) for all methods on a type.
- Use `json:"camelCase"` struct tags on all wire-format structs. No JSON tags on user-facing config structs.
- Doc comments must start with the type/function name (Go `godoc` convention).
- Run `go vet` and `go fmt` before committing.
- Unit tests go in `_test.go` files alongside the source they test. Use table-driven tests with `t.Run`.
- Samples must NOT commit `go.mod`, `go.sum`, or compiled binaries — users generate these via the README.
- Each sample needs: `main.go`, `host.json`, `local.settings.json`, `README.md`.

## Build & Test

    # Run all tests
    go test ./...

    # Run a sample locally
    cd samples/httpTrigger
    go mod init myapp
    go get github.com/azure/azure-functions-golang-worker
    go mod tidy
    func start   # requires: npm i -g @gaaguiar/azure-functions-core-tools
