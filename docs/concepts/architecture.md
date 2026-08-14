# Architecture

The Azure Functions host and compiled Go Function App run as separate
processes. The host manages triggers and invocation orchestration. The Go app
contains the worker SDK, registered functions, bindings, middleware, and
customer code.

They communicate through the bidirectional `FunctionRpc.EventStream` gRPC
connection.

## Explore the Host-Worker Protocol

The visualization separates startup and host negotiation from customer request
processing. Choose a story and phase to follow the primary message flow.

<iframe
  src="../../assets/host-worker-visualization.html"
  title="Azure Functions host and Go worker protocol visualization"
  style="width: 100%; height: 920px; border: 0; border-radius: 12px;"
  loading="lazy">
</iframe>

<small><a href="../../assets/host-worker-visualization.html">Open visualization</a></small>

## Responsibilities

| Azure Functions host | Go Function App |
| --- | --- |
| Listens for triggers | Registers functions in code |
| Orchestrates invocations | Reports function metadata |
| Sends binding data | Converts inputs into Go values |
| Applies output bindings | Runs middleware and customer code |
| Manages process lifecycle | Returns results and logs |

## Lifecycle at a glance

### Startup

1. The host launches the compiled Go app.
2. The app opens the gRPC stream and sends `StartStream`.
3. The host initializes the app with `WorkerInitRequest`.
4. The app reports its capabilities and registered function metadata.
5. The host loads each function by ID.

### Invocation

1. A trigger reaches the host.
2. The host sends an `InvocationRequest`.
3. The app prepares the invocation context and converts binding inputs.
4. Middleware and customer code execute.
5. Logs and the `InvocationResponse` return through the gRPC stream.

## Important variations

The interactive diagram shows the primary gRPC payload path. Some workloads use
additional mechanisms:

- Core trigger payloads travel inline in `InvocationRequest`.
- Extension triggers may receive metadata and an injected Azure SDK client.
- HTTP streaming can use an additional loopback HTTP path.
- Consumption deployments launch the app through the worker proxy.

## Related documentation

- [Triggers and bindings](triggers-and-bindings.md)
- [Logging and observability](../guides/observability.md)
- [Error and panic handling](../guides/error-handling.md)
- [Deployment](../guides/deployment.md)
- [Technical specification](https://github.com/Azure/azure-functions-golang-worker/blob/main/TECHNICAL_SPEC.md)
