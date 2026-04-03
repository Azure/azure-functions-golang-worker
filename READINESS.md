# Azure Functions Go Worker — Readiness Checklist

## Before Preview

These items affect API surface, correctness, or developer experience and should
be addressed before any external users evaluate the programming model.

### Must Fix

- [ ] **Remove or mark `WithRetry()` as not-yet-implemented**
  `RetryOptions` is defined in `sdk/retry.go` and `WithRetry()` is callable on
  `RegisteredFunction`, but retry options are never serialized into
  `RpcFunctionMetadata` sent to the host. Users who call `WithRetry()` get
  silent no-op behavior. Either wire it through to the proto response or panic
  with "not yet implemented" to avoid misleading users.
  Files: `sdk/retry.go`, `sdk/app.go`, `worker/handlers.go`

- [ ] **Remove `RegisterFunction()` export (keep only `RegisterFunctionWithName`)**
  `RegisterFunction(f, b)` passes empty name and was only needed by the old
  `blobtrigger.Register()` pattern which no longer exists. Having two exported
  registration methods is confusing. Consolidate to `RegisterFunctionWithName`
  or remove the parameterless variant.
  File: `sdk/app.go`

- [ ] **Mark httpStreaming sample as not-yet-supported**
  `ResponseWriterProxy` does not implement `http.Flusher`, so the streaming
  sample always returns HTTP 500 "Streaming not supported". Add a clear comment
  at the top of the sample and in the README, or remove the sample until
  streaming is implemented.
  File: `samples/httpStreaming/main.go`

- [ ] **Add `InvocationCancel` handling + context propagation**
  All invocations use `context.Background()` — no cancellation propagation from
  the host. The host sends `InvocationCancel` messages which the worker ignores.
  Implement: create `context.WithCancel()` per invocation, store in a map keyed
  by invocation ID, cancel when `InvocationCancel` is received. This is a
  separate PR but must land before preview since handlers that use `ctx.Done()`
  will silently malfunction without it.
  File: `worker/handlers.go`

### Should Fix

- [ ] **Replace `log.Fatalf` in gRPC loop with error propagation**
  `handleBidiStream` in `worker/grpc.go` calls `log.Fatalf` on recv/send errors,
  which terminates the process. Transient gRPC errors should be retried or
  returned as failure status, not kill the worker.
  File: `worker/grpc.go`

- [ ] **Implement `handleWorkerTerminate`**
  Currently returns `nil, nil` — no cleanup, no process exit. The host sends
  this to request graceful shutdown. At minimum, close the gRPC stream and exit
  cleanly.
  File: `worker/handlers.go`

- [ ] **Validate handler signature at registration time for blob trigger**
  `app.Blob()` accepts `any` handler. If the handler has the wrong signature
  (e.g., wrong argument type), the error only surfaces at invocation time as a
  reflect panic. Add type validation in `registerFunction` when `ClientFactory`
  is set.
  File: `sdk/app.go`

---

## Before GA

These items are important for production readiness but acceptable for a preview
where users are evaluating the programming model.

### Must Fix

- [ ] **Worker concurrency control**
  No max concurrency setting. The host can send unlimited concurrent invocations
  and the worker will attempt to handle all of them. Add a configurable
  concurrency limit (semaphore or worker pool).
  File: `worker/handlers.go`

- [ ] **Batch/cardinality "many" support**
  Builder methods accept `Cardinality("many")` for Service Bus and Event Hub but
  the dispatcher has not been tested with array payloads. Verify and implement
  batch handler signatures (e.g., `func(ctx, []ServiceBusMessage) error`).
  Files: `sdk/handlers.go`, `worker/converter.go`, `worker/handlers.go`

- [ ] **Worker capabilities advertisement**
  `WorkerInitResponse` does not set any capabilities. The host checks for
  capabilities like `RpcHttpTriggerMetadataRemoved`, `HandlesWorkerTerminateMessage`,
  etc. Without these, the host may send unnecessary data or not send expected
  messages.
  File: `worker/handlers.go`

- [ ] **HTTP streaming support**
  Implement `http.Flusher` on `ResponseWriterProxy` or switch to the host's
  HTTP proxy mode for streaming scenarios.
  File: `worker/http_helpers.go`

- [ ] **Wire `RetryOptions` to the host**
  If not removed before preview, implement the full retry serialization into
  `RpcRetryOptions` in the function metadata response.
  Files: `sdk/retry.go`, `worker/handlers.go`

- [ ] **Telemetry / OpenTelemetry support**
  No distributed tracing. Production workloads need trace context propagation
  and integration with Azure Monitor.

- [ ] **`FunctionEnvironmentReloadRequest` should reload env vars**
  Currently returns success without actually reloading. The host sends this
  when app settings change. The worker should re-read environment variables.
  File: `worker/handlers.go`

### Should Fix

- [ ] **Re-export key types from `sdk` package**
  Users currently need two imports: `sdk` for builders and `sdk/bindings` for
  trigger payload types (`TimerInfo`, `ServiceBusMessage`, etc.). Consider
  re-exporting the key types from `sdk` to reduce import boilerplate.
  File: `sdk/handlers.go` or new `sdk/types.go`

- [ ] **Input validation at registration time**
  Missing `.Schedule()` on Timer, missing `.Connection()` on ServiceBus, etc.
  are only caught at runtime by the host. Consider adding `Build()` or
  `Validate()` methods on builders.
  File: `sdk/app.go`

- [ ] **Multi-function sample**
  Add a sample showing HTTP + Timer + Service Bus triggers in one binary to
  demonstrate the multi-function pattern.
  Directory: `samples/multiTrigger/`

- [ ] **`EventGridBinding` empty struct serialization**
  `EventGridBinding` is `struct{}` which serializes as `{}` and adds empty
  fields to the binding JSON. Consider using `nil` pointer instead to avoid
  empty object in serialized output.
  File: `sdk/bindings/eventgrid.go`

- [ ] **Shared memory / large message support**
  The proto supports `RpcSharedMemory` for large payloads. Not implemented.
  Would improve performance for large blob/cosmos payloads.

- [ ] **Function warm-up handler**
  `WorkerWarmupRequest` is defined in the proto but not handled by the
  dispatcher. The host sends this to pre-warm the worker.
  File: `worker/dispatcher.go`
