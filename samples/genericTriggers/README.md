# Generic triggers

This sample compares direct `App.GenericTrigger` registrations with a small
typed package built on the same API. Use the local checkout until a worker
release containing `GenericTrigger` is available.

## No custom package required

Applications can register directly using only the worker's SDK and binding
descriptor. There is no user-authored trigger type, package, or blank import.

```go
app.GenericTrigger("ProcessOrder", func(ctx context.Context, body []byte) error {
  slog.InfoContext(ctx, "order", "body", string(body))
  return nil
}, &bindings.GenericTrigger{
  Type: "queueTrigger", Name: "message", DataType: "string",
  Properties: map[string]any{
    "queueName": "orders", "connection": "AzureWebJobsStorage",
  },
})
```

For JSON decoding, replace `[]byte` with your application's struct type. The
host extension still needs to be installed, normally through the extension
bundle. That is separate from a Go trigger package.

## Scenarios

Set `GENERIC_SCENARIO` before starting the application.

| Value | Functions | What it demonstrates |
| --- | --- | --- |
| `queues` (default) | RawQueue, JSONQueue, TypedQueue, MetadataQueue | Raw bytes, JSON models, a typed registration package, invocation metadata |
| `eventhub` | BatchOrders | An explicit host batch decoded into `[]Order` |
| `mcp` | EchoTool | An unfamiliar trigger type and a return value consumed by the host extension |

`JSONQueue` and `TypedQueue` use the same `handleOrder` function. The sample's
`queue` package adds configuration types and validation, then delegates to
`GenericTrigger`. It does not install middleware, create a client, or replace
the handler. The compiler infers `T` from the handler.

The example middleware is shared by all functions. Normal middleware, tracing,
and invocation error handling still apply.

## Inspect without external services

From the repository root, using its CI-equivalent Go workspace:

```powershell
go test ./sdk/... ./worker/... ./samples/genericTriggers/...
go run ./samples/genericTriggers --metadata
$env:GENERIC_SCENARIO = 'eventhub'
go run ./samples/genericTriggers --metadata
$env:GENERIC_SCENARIO = 'mcp'
go run ./samples/genericTriggers --metadata
```

The metadata mode does not start a Functions host or prove extension
compatibility. Worker tests exercise real metadata, load, and invocation
handlers with protobuf requests, including an unknown `unfamiliarTrigger`.

## Run with Core Tools

The sample shares the root module. Do not initialize another Go module here.
Build from the repository root, then start the prebuilt application with a
Go-capable Core Tools installation:

```powershell
go build -o ./samples/genericTriggers/bin/app.exe ./samples/genericTriggers
$env:FUNCTIONS_WORKER_RUNTIME = 'native'
$env:FUNCTIONS_CLI_NATIVE_LANGUAGE = 'go'
$env:GENERIC_SCENARIO = 'queues'
$env:AzureWebJobsStorage = 'UseDevelopmentStorage=true'
$env:GenericRawQueue = 'generic-raw'
$env:GenericJSONQueue = 'generic-json'
$env:GenericTypedQueue = 'generic-typed'
$env:GenericMetadataQueue = 'generic-metadata'
Push-Location ./samples/genericTriggers
func start --no-build
Pop-Location
```

Use `bin/app` rather than `bin/app.exe` on Linux/macOS. Start Azurite first for
the queue scenario. Send base64-encoded queue messages, as expected by the
Storage Queues extension. A JSON example is `{"id":"order-1","quantity":2}`.
Use separate queues so the demonstrations do not compete for messages.

The Event Hubs scenario needs `OrdersEventHub` and `OrdersEventHubConnection`.
Each event body should be a JSON order. No Event Hubs resource is provisioned
by this sample.

The MCP scenario needs a bundle containing the MCP extension and a compatible
host/runtime. Its connection settings and transport authentication are owned
by that extension. The tool is `echo` with the required text argument `text`.
No generic Go declaration installs a missing host extension.

## Contract and limits

- `Type`, `Name`, optional `DataType` (`string` or `binary`), optional
  `Cardinality` (`one` or `many`), and JSON-compatible `Properties` describe the
  trigger. Properties are flattened using the host's exact JSON names.
- Registration validates signatures and metadata, then snapshots generic
  property maps, including nested values. Reserved-key collisions are errors.
  Treat registered metadata as immutable.
- Handlers take `(context.Context, T)` and return `error` or `(R, error)`.
  `T` must be raw text, raw bytes, or JSON-decodable, and `R` must be raw
  text, raw bytes, or JSON-encodable. Registration rejects types that JSON
  cannot represent, such as channels, functions, non-empty interface payloads
  like `io.Reader`, and map keys JSON cannot use. Custom JSON and text
  codecs are allowed. Struct fields follow `encoding/json` rules and fail per
  invocation. Pointer-only recursive types such as `type P *P` are rejected
  anywhere in a payload, because decoding one can loop forever.
  Text and byte slices preserve the payload, including JSON quoting. Other
  types use JSON decoding. Pointer models can receive JSON null. Matching
  string/byte representations avoid payload-sized copies; treat input bytes
  as read-only.
- Dynamic JSON fields (`any`, `map[string]any`, and nested interface fields)
  receive numbers as `json.Number`, not `float64`. Use `Int64`, `Float64`, or
  `String` explicitly. For example, an ID of `9007199254740993` remains exact.
  Typed numeric fields retain their declared Go type. A host-supplied double
  already has floating-point precision; it cannot recover lost source digits.
- Raw string/byte types bypass custom `UnmarshalJSON`; `json.Number` is treated
  as a number, not raw text. For other types, custom JSON decoders take control.
  A custom named-slice decoder receives a JSON array even for a native host
  collection. Raw text elements become JSON strings, raw byte elements become
  base64 JSON strings, and structured elements remain JSON. The custom batch
  decoder owns null handling, element validation, and its own number policy.
  Return encoding sends no value for a nil pointer or interface result.
  Otherwise it honors `json.Marshaler` and `encoding.TextMarshaler` first,
  then follows pointers, so `*string` and `*[]byte` return text and bytes like
  `string` and `[]byte`. Text and JSON results must be valid UTF-8, so return
  `[]byte` for binary data.
- Host collections decode element by element into slices, preserving empty
  entries. An ordinary JSON array does not automatically request host batching.
- `TriggerMetadataValues` preserves primitive values, primitive collections,
  and JSON as `json.RawMessage`. The old string-only metadata map is unchanged.
  This richer map is currently populated only for generic trigger registrations.
- Auxiliary inputs can be read using the existing middleware binding-input
  accessors. GenericTrigger does not add a public input-binding helper or
  arbitrary multi-parameter handler mapping.
- A return value needs a consumer, either the trigger extension (MCP, for
  example) or a declared `$return` output binding. Named multiple outputs are
  not implemented. Serialization and input conversion errors fail invocation.
- Use `App.HTTP` for HTTP. Generic registration does not automatically select
  blob client factories or implement Durable replay, SDK clients, deferred
  bindings, shared-memory payloads, or message settlement.
- Trigger-specific options such as `WithQueueName` still target their typed
  models. Supply their settings through `Properties` for generic registrations.

Unset/unsupported metadata wire kinds are omitted. The test suite enumerates
the protobuf oneof so newly added wire kinds need an explicit support decision.

## Local verification

Verified on Windows with Core Tools 4.12.0 and host 4.1048.200.26180:

- All four queue functions received messages from local Azurite and completed
  successfully, including the direct JSON and typed-package registrations.
- `EchoTool` indexed as `mcpToolTrigger`. A real MCP initialize, tools/list, and
  tools/call exchange returned the supplied text through the host extension.
- Event Hubs batch conversion is covered by worker tests. This sample has not
  been exercised against a live Event Hub.

The committed integration tests repeat the queue and MCP checks using
temporary app directories, unique queues and host identities, and cleanup of
only their own resources. With Azurite and Core Tools installed, run from
`tests/integration`:

```powershell
$env:GOWORK = 'off'
go test -v -count=1 -run '^TestGenericTrigger(Queues|MCP)$' .
```

Both tests are included in the integration CI runner. The SDK examples and
sample package tests also run in the unit-test job.

These checks do not establish cloud scaling behavior or compatibility with
every extension, bundle version, or hosting plan.