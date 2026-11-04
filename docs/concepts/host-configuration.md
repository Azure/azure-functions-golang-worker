# Host configuration delivery

When a host sends the exact, case-sensitive capability
`HostConfiguration = "1"` in `WorkerInitRequest`, the Go worker delivers a
snapshot of its local `host.json` in `WorkerInitResponse.host_configuration`.
Other values, including an absent capability, omit the field without looking
up the application directory, opening the file, initializing the snapshot, or
emitting configuration-delivery diagnostics.
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
| `Available` | The file was read, converted, and normalized to a nonempty JSON object document; `{}` is valid. |
| `Missing` | Opening `host.json` reported a missing file in an accessible application directory. |
| `Error` | The root or parent directory was unavailable, or opening, reading, closing, decoding, or JSON normalization failed. |
| `TooLarge` | The converted or normalized UTF-8 output exceeded 65,536 bytes. |

Only `Available` carries content. An omitted envelope is not a `Missing`
outcome. `Error` and `TooLarge` do not themselves turn a successful worker
initialization result into a failure; the Host independently accepts or rejects
the delivery. Genuine worker startup failures retain their existing behavior.
Configuration content and path-bearing acquisition errors are not added to
logs, initialization results, metadata, or capabilities.

The default encoding is strict UTF-8. UTF-8, UTF-16 LE/BE, and UTF-32 LE/BE
BOMs are recognized and removed; conversion rejects malformed or truncated
sequences instead of replacing them. Reading and conversion are incremental
and bounded. Valid U+FFFD characters are preserved, with or without a BOM.
The converted BOM-free UTF-8 must fit within 65,536 bytes **before**
normalization, regardless of source file size. The normalized output is also
limited to 65,536 bytes inclusive. Oversized input cannot become acceptable by
removing whitespace or comments, even if its JSON is malformed. Failure
responses never contain partial content.

## JSON normalization and diagnostics

The worker validates JSON syntax and emits a compact, strict JSON object. It accepts
`//` and `/* */` comments and trailing commas, removing them along with
insignificant whitespace. Exactly one object root is required, with at most
64 nested containers including the root. Nested arrays and scalar values remain
valid. Property order, duplicate keys, raw string
and property escape spellings (including escaped lone surrogates), and raw
number spellings and precision are preserved without conversion to Go values.
Only ASCII space, tab, CR, and LF count as JSON whitespace. Single-line
comments end at CR, LF, or EOF and reject U+2028/U+2029; those characters are
allowed inside block comments and JSON strings.

An empty decoded document, including a BOM-only file, is `Error` with reason
`InvalidJson` and no content. Whitespace-only, comment-only, malformed JSON,
and non-object roots (including after comments) have the same outcome. The
empty object `{}` and objects containing nested arrays or scalars remain valid.
The worker does not apply defaults
or merge environment settings into JSON. Host-specific semantic validation,
defaults, and app-setting override precedence remain owned by the Host.

When an `Error` or `TooLarge` snapshot is first cached, the worker emits one
system warning, subject to normal Host log filtering:

> Host configuration could not be delivered to the host. Reason: {Reason}.
> The failure is retained until the worker process restarts.

The reason is a fixed code: `AppDirectoryNotSet`, `AppDirectoryNotFound`,
`AccessDenied`, `OpenFailed`, `ReadFailed`, `InvalidEncoding`, `InvalidJson`,
or `Oversized`. Go's environment lookup and path joining do not return the
accessor/join exceptions represented by the reference implementation's
`AppDirectoryUnavailable` and `AppDirectoryInvalid` reasons. Native open
failures, including invalid paths, are classified at open time instead.
Read or close failures are always errors, even if they report a file that
disappeared after opening. `Available`, `Missing`, disabled requests, and
cached replays do not emit failure warnings. No warning contains a path,
configuration value, document fragment, or underlying exception message.

## Protocol maintenance

The vendored delivery schema comes from
[azure-functions-language-worker-protobuf#103](https://github.com/Azure/azure-functions-language-worker-protobuf/pull/103),
commit `313666583af4e97f0b534f1cbf61a3812f277c12`. It uses response field 6,
status field 1, and content field 2. Preserve this repository's `go_package`
option when updating the schema.

Normalization and diagnostics follow
[azure-functions-dotnet-worker#3522](https://github.com/Azure/azure-functions-dotnet-worker/pull/3522)
at commit `e7765e7201008340e4cc407486bbd31649abf162`, without changing the wire
schema. The Go implementation uses strict decoders rather than the reference's
StreamReader replacement-character workaround.

Regenerate `worker/proto/FunctionRpc.pb.go` with protoc 34.1 and
`protoc-gen-go` v1.36.4, matching the existing generated bindings. From the
repository root, with the generators on `PATH`:

```powershell
protoc --proto_path=worker\proto --proto_path="<protoc include directory>" --go_out=worker\proto --go_opt=paths=source_relative worker\proto\FunctionRpc.proto
```

No service or imported message changed, so the gRPC service binding and the
identity/nullable bindings do not need regeneration.
