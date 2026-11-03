# Host configuration delivery

When a host sends the exact, case-sensitive capability
`HostConfiguration = "1"` in `WorkerInitRequest`, the Go worker delivers a
snapshot of its local `host.json` in `WorkerInitResponse.host_configuration`.
Other values, including an absent capability, omit the field without looking
up the application directory, opening the file, or initializing the snapshot.
Existing initialization results and worker/middleware capabilities are unchanged.

## Application directory and lifetime

The launcher must set `FUNCTIONS_APPLICATION_DIRECTORY` in the worker process.
The worker reads `host.json` under that directory, not the Host-supplied
`function_app_directory`, the worker installation directory, or an implicit
current-directory fallback. Explicit relative roots retain native platform
resolution. An unset, empty, or whitespace-only root is an error.

The first enabled initialization captures one immutable snapshot for the
process lifetime, including unsuccessful outcomes. Repeated or concurrent
initializations, including those handled by a newly created dispatcher, replay
that snapshot in independent response envelopes and byte slices. Disabled
requests still omit the field after capture. There is no watcher, live reread,
or retry of a cached failure; restart the worker to capture changed content.

## Outcomes and encoding

| Status | Meaning |
| --- | --- |
| `Available` | The file was read and converted successfully, including an empty file. |
| `Missing` | `host.json` was not found in an accessible application directory. |
| `Error` | The root or parent directory was unavailable, or opening, reading, closing, or decoding failed. |
| `TooLarge` | The converted UTF-8 output exceeded 65,536 bytes. |

Only `Available` carries content. An omitted envelope is not a `Missing`
outcome. `Error` and `TooLarge` do not themselves turn a successful worker
initialization result into a failure; the Host independently accepts or rejects
the delivery. Genuine worker startup failures retain their existing behavior.
Configuration content and path-bearing acquisition errors are not added to
logs, initialization results, metadata, or capabilities.

The default encoding is strict UTF-8. UTF-8, UTF-16 LE/BE, and UTF-32 LE/BE
BOMs are recognized and removed; conversion rejects malformed or truncated
sequences instead of replacing them. Reading and conversion are incremental
and bounded. The limit applies to the final BOM-free UTF-8 bytes, not the
source file size: 65,536 bytes are allowed and 65,537 bytes are too large.
Failure responses never contain partial content.

The worker does not parse JSON, apply defaults, or merge environment settings
into the payload. Empty content and malformed JSON are transported for the
Host to validate. JSON validation, defaults, and app-setting override
precedence belong to the Host.

## Protocol maintenance

The vendored delivery schema comes from
[azure-functions-language-worker-protobuf#103](https://github.com/Azure/azure-functions-language-worker-protobuf/pull/103),
commit `313666583af4e97f0b534f1cbf61a3812f277c12`. It uses response field 6,
status field 1, and content field 2. Preserve this repository's `go_package`
option when updating the schema.

Regenerate `worker/proto/FunctionRpc.pb.go` with protoc 34.1 and
`protoc-gen-go` v1.36.4, matching the existing generated bindings. From the
repository root, with the generators on `PATH`:

```powershell
protoc --proto_path=worker\proto --proto_path="<protoc include directory>" --go_out=worker\proto --go_opt=paths=source_relative worker\proto\FunctionRpc.proto
```

No service or imported message changed, so the gRPC service binding and the
identity/nullable bindings do not need regeneration.
