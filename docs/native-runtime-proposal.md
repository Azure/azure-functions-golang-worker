# Proposal: "Native" Runtime Image for Azure Functions

## Problem

The Azure Functions Go worker is a gRPC-based language worker, not a custom handler, but the runtime image it ships on does not need to include a language-specific runtime. Go compiles to a static binary, and the same is true for Rust, C++, and any other language that produces native executables. Creating a dedicated "golang" runtime pool introduces two problems:

1. **Confusing version semantics.** When a user selects `golang 1.0`, the `1.0` refers to the OS image revision, not the Go language version. This is unlike every other stack (Python 3.12, Node 20, Java 17) where the version maps directly to the runtime installed in the image. The confusion gets worse when image versions inevitably overlap with real language versions (e.g., Go 1.24 vs. image revision 1.0).

2. **Unsustainable proliferation.** Every new compiled language (Rust, C++, Zig, etc.) would require its own SKU, pool, pipeline registration, Ev2 rollout configuration, and MCR syndication, all for images that are functionally identical.

## Proposal

Introduce a single runtime image and worker pool called **`native`** that serves all compiled-to-native-binary language workers.

| Attribute | Value |
|---|---|
| `FUNCTIONS_WORKER_RUNTIME` | `native` |
| `FUNCTIONS_WORKER_RUNTIME_VERSION` | `1.0` |
| Worker protocol | gRPC bidirectional streaming (standard `FunctionRpc.EventStream`) |
| Fixed executable name | `app` |
| Worker config | Written inline in the Dockerfile (not shipped with the host) |
| Initial SKUs | Flex Consumption, then Linux Dedicated (both on Noble) |

The key difference from a custom handler is the protocol: custom handlers use HTTP invocation (`HttpFunctionInvocationDispatcher`), while native workers use the full gRPC language-worker protocol (`RpcFunctionInvocationDispatcher`) with worker indexing, typed invocation requests, and capability negotiation. This is the same protocol that Python, Node, and Java workers implement.

## Benefits

### 1. Accurate User Experience

Users select a runtime that describes what is actually in the image: an OS with the Functions Host and nothing else. The version (`1.0`) is an opaque image revision that abstracts away the underlying OS. Users do not need to know or care which Linux distribution is in the image. Compare:

| Today (per-language) | Proposed (native) |
|---|---|
| `--runtime golang --runtime-version 1.0` | `--runtime native --runtime-version 1.0` |
| *"Is 1.0 the Go version?"* | *"The image ships an OS and the host. I bring my binary."* |

### 2. Instant Support for New Languages

When a Rust or C++ worker is developed, it is immediately operational in production with no new image, pool, pipeline, or Ev2 rollout required. The worker just needs to:

- Compile to a Linux binary named `app`
- Implement the FunctionRpc gRPC protocol

### 3. Enables Native Builds of Managed-Runtime Languages

The `native` runtime opens the door to supporting ahead-of-time compiled builds of languages that normally use a managed runtime. Users who produce native/AOT executables from Java (GraalVM native-image), .NET (NativeAOT/`PublishAot`), or Python (Nuitka/PyInstaller) could deploy them to a `native` app. Because the image contains no managed runtime, there is nothing to conflict with and no unnecessary runtime overhead. The user's self-contained binary runs exactly as it would on any Linux host.

This is not a goal for the initial release, but the `native` image makes it possible without any additional image or pool work.

### 4. Reduced Operational Burden

One image per SKU replaces *N* images per compiled language. One release-configuration entry, one Ev2 rollout, one MCR syndication config.

## Image Contents

The native image is nearly identical to the existing custom handler image. The differences are:

| | Custom Handler | Native |
|---|---|---|
| `FUNCTIONS_WORKER_RUNTIME` | `custom` | `native` |
| Invocation protocol | HTTP (`HttpFunctionInvocationDispatcher`) | gRPC (`RpcFunctionInvocationDispatcher`) |
| Function discovery | `function.json` files on disk | Worker indexing via gRPC (`FunctionsMetadataResponse`) |
| Worker config | None (not a language worker) | Written inline in the Dockerfile |
| Executable name | User-defined via `host.json` | Fixed: `app` |

## Fixed Executable Name

All apps deployed to the native runtime must produce a binary named `app`. This is a deliberate constraint:

- **No per-language build tooling required.** A flexible executable name requires generating `worker.config.json` at build time, which means enforcing build steps and tooling (`func pack`, post-build scripts, etc.) that serve no purpose other than producing a JSON file. Every language would need its own generator, adding complexity for zero functionality gain.
- **Matches industry precedent.** AWS Lambda's OS-only runtimes (`provided.al2023`) require all executables to be named `bootstrap`. This is universally accepted and causes no friction.
- **The name is invisible to end users.** The output binary name is a build artifact detail, not a product feature. Users configure it once in their build command (e.g., `go build -o app .`) and never think about it again.

## Worker Config in the Dockerfile

The `worker.config.json` is written inline in the Dockerfile rather than shipped as part of the Functions Host. This decouples updates to the worker configuration from host release cycles. If the worker config needs to change (e.g., adjusting timeouts or adding capabilities), it can be updated with a Docker image rebuild instead of waiting for a new host release.

## Dockerfile Examples

### Flex Consumption (Noble)

```dockerfile
ARG HOST_BASE_IMAGE
FROM ${HOST_BASE_IMAGE} AS runtime

# Write the worker config inline so updates do not require a host release.
RUN mkdir -p /azure-functions-host/workers/native && \
    echo '{ \
        "description": { \
            "language": "native", \
            "extensions": [], \
            "supportedOperatingSystems": ["LINUX"], \
            "supportedArchitectures": ["X64", "Arm64"], \
            "defaultExecutablePath": "app", \
            "workerIndexing": "true" \
        }, \
        "processOptions": { \
            "initializationTimeout": "00:02:00", \
            "environmentReloadTimeout": "00:02:00" \
        } \
    }' > /azure-functions-host/workers/native/worker.config.json

ENV FUNCTIONS_WORKER_RUNTIME=native \
    FUNCTIONS_WORKER_RUNTIME_VERSION=1.0
```

### Linux Dedicated (Noble)

```dockerfile
ARG HOST_BASE_IMAGE
FROM ${HOST_BASE_IMAGE} AS runtime

RUN mkdir -p /azure-functions-host/workers/native && \
    echo '{ \
        "description": { \
            "language": "native", \
            "extensions": [], \
            "supportedOperatingSystems": ["LINUX"], \
            "supportedArchitectures": ["X64", "Arm64"], \
            "defaultExecutablePath": "app", \
            "workerIndexing": "true" \
        }, \
        "processOptions": { \
            "initializationTimeout": "00:02:00", \
            "environmentReloadTimeout": "00:02:00" \
        } \
    }' > /azure-functions-host/workers/native/worker.config.json

ENV FUNCTIONS_WORKER_RUNTIME=native \
    ASPNETCORE_URLS=http://+:80

CMD [ "/opt/startup/start_nonappservice.sh" ]
```

### Worker Config Contents

The inline JSON written by both Dockerfiles produces the following `worker.config.json`:

```json
{
    "description": {
        "language": "native",
        "extensions": [],
        "supportedOperatingSystems": ["LINUX"],
        "supportedArchitectures": ["X64", "Arm64"],
        "defaultExecutablePath": "app",
        "workerIndexing": "true"
    },
    "processOptions": {
        "initializationTimeout": "00:02:00",
        "environmentReloadTimeout": "00:02:00"
    }
}
```

The host discovers this file at `workers/native/worker.config.json`, starts the `app` executable with `--functions-uri`, `--functions-worker-id`, and other standard gRPC flags, and communicates over the same bidirectional streaming protocol used by all language workers.

## AWS Precedent

AWS Lambda offers `provided.al2023` and `provided.al2`, OS-only runtimes with no language-specific software installed. This is the standard deployment target for Go, Rust, C++, and any other compiled language on Lambda.

| | AWS Lambda `provided` | Azure Functions `native` (proposed) |
|---|---|---|
| Image contents | Amazon Linux + Lambda Runtime API | Ubuntu Noble + Functions Host |
| Worker protocol | HTTP polling (Runtime API) | gRPC bidirectional streaming |
| Fixed executable name | `bootstrap` | `app` |
| Shared across languages | Yes (Go, Rust, C++, etc.) | Yes (Go, Rust, C++, etc.) |
| Version semantics | OS version (`al2023`) | Image revision (`1.0`) |

## What Changes

| Component | Change Required |
|---|---|
| **Docker images** | Add `native1.Dockerfile` for Flex Consumption and Linux Dedicated (2 files, ~15 lines each, Noble only) |
| **Pipeline config** | Add `native1` identifier to `build_validate_publish.yml` |
| **Release config** | No change (uses existing host version and docker revision) |
| **Functions Host** | None. The worker config is written in the Dockerfile; the host already supports arbitrary language names via `worker.config.json` discovery. |
| **Core Tools** | Add `native` to `WorkerRuntime` enum and init templates |


## Summary

A `native` runtime replaces per-language images for compiled languages with a single, reusable image. It abstracts the underlying OS from the user behind a simple image revision (`1.0`), eliminates operational overhead for new languages, and follows the same pattern that AWS Lambda has validated at scale with `provided` runtimes. The worker config is written inline in the Dockerfile to allow updates without waiting for host releases.
