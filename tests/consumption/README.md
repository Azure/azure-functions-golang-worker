# Flex Consumption Tests

End-to-end tests for the Go worker running in Azure Functions flex consumption containers. Tests use Docker to start real flex consumption images with the proxy, deploy apps, specialize, and verify HTTP triggers work.

## Prerequisites

- Docker Desktop running
- Go 1.24+

## Tests

| Test | Description |
|------|-------------|
| `TestFlexConsumptionPlaceholderPing` | Verifies the proxy handles placeholder mode — container returns 200 on `/admin/host/ping` |
| `TestFlexConsumptionHttpTriggerProxy` | Full lifecycle: placeholder → deploy → specialize → HTTP trigger returns 200 |

Run tests:

```sh
cd tests/consumption
go test -v -timeout 300s -run "TestFlexConsumption" .
```

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkHttpTriggerDirect` | Baseline — app prebaked in image, proxy execs into it (no proxy overhead) |
| `BenchmarkHttpTriggerProxy` | Production path — proxy handles placeholder, spawns child on specialization |

Run benchmarks:

```sh
cd tests/consumption
go test -timeout 600s -run "^$" -bench "BenchmarkHttpTrigger" -benchtime 30s -count=3 .
```

## Dockerfiles

| File | Purpose |
|------|---------|
| `Dockerfile.flex-test` | Main test image — proxy baked in, app deployed at runtime |
| `Dockerfile.flex-test-direct-bench` | Benchmark baseline — proxy + app both baked in (exec bypass on startup) |

Both use `mcr.microsoft.com/azure-functions/bookworm/flexconsumption:4.1047.100-0-custom1.0` as the base image.

## Architecture

```
┌─────────────────────────────────────────────────┐
│ Flex Consumption Container                      │
│                                                 │
│  Functions Host ◄──gRPC──► Proxy ◄──gRPC──► App │
│       :80              (localhost)               │
│                                                 │
│  After restart (exec bypass):                   │
│  Functions Host ◄──gRPC──► App                  │
│       :80           (direct, zero overhead)      │
└─────────────────────────────────────────────────┘
```

## How Tests Work

1. Build Docker image from Dockerfile (proxy compiled from source)
2. Start container with `WEBSITE_PLACEHOLDER_MODE=1`
3. Wait for `/admin/host/ping` to return 200 (placeholder ready)
4. Cross-compile the httpTrigger sample, zip it, deploy via `docker cp`
5. Send encrypted specialization payload to `/admin/instance/assign`
6. Wait for `GET /api/hello` to return 200
7. Assert response body is `"Hello from Go Worker!"`
