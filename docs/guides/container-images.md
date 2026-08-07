# Build and package for the platform image

Azure Functions runs Go applications on a platform-provided Ubuntu image. You
deploy your application; Azure maintains the Functions host and container
image.

You don't need a `Dockerfile` or container registry.

!!! note
    Go support is currently in preview on the
    [Flex Consumption plan](https://learn.microsoft.com/azure/azure-functions/flex-consumption-plan).

## Platform image

| Platform runtime | Runtime version | Base OS | Architecture | Image pattern | Description |
| --- | --- | --- | --- | --- | --- |
| `native` | `1.0` | Ubuntu Noble | Linux x64 | `mcr.microsoft.com/azure-functions/noble/flexconsumption:<HOST_VERSION>-native1.0` | Azure Functions host image for native executables, including Go applications. |

Azure selects and updates the host version. You don't select or deploy this
image.

## Build and package

### Build with Core Tools

From the function project root, create a deployment package:

```bash
func pack --output functionapp.zip
```

Core Tools builds the Go application for Linux x64 and packages it with the
required Functions files.

### Package a prebuilt executable

!!! important
    The executable must be named `app`.

To package an executable that you built yourself:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o app .
func pack --no-build --output functionapp.zip
```

Core Tools marks `app` as executable in the ZIP metadata.

To create the ZIP directly on Linux:

```bash
FILES=(app host.json)
[ -f worker.config.json ] && FILES+=(worker.config.json)
zip -r functionapp.zip "${FILES[@]}"
```

See [Deployment](deployment.md) for supported deployment methods.

## Related documentation

- [Getting started](../getting-started.md)
- [Azure Functions Core Tools](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
