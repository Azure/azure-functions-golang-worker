# Roadmap

Native Go hosting on Flex Consumption is generally available. Version 1.x of
the Go worker provides stable public APIs. This page shows the current support
surface and areas being explored.

## Hosting plans

| Plan | Status |
| --- | --- |
| Flex Consumption | Generally available |
| Elastic Premium | On the roadmap |
| Dedicated (Linux App Service plan) | On the roadmap |

Go applications must target Linux x64 with CGO disabled
(`CGO_ENABLED=0`). Windows is not supported.

## Triggers

| Trigger | Status | Notes |
| --- | --- | --- |
| HTTP | Supported | Includes HTTP streaming |
| Timer | Supported | |
| Azure Queue Storage | Supported | |
| Azure Blob Storage | Supported | Uses Azure SDK client injection |
| Azure Cosmos DB | Supported | |
| Azure Event Grid | Supported | |
| Azure Event Hubs | Supported | |
| Azure Service Bus queues and topics | Supported | |
| Azure SQL | Supported | |
| Generic host triggers | Supported | Requires a compatible installed host extension |

## Capabilities

| Capability | Status | Notes |
| --- | --- | --- |
| Deferred bindings | On the roadmap | SDK-typed trigger and input bindings |
| Durable Functions | Supported | Orchestrations, activities, and management clients; entities are not supported |

!!! tip "Help shape the roadmap"
    Don't see the hosting plan, trigger, binding, or feature you need?
    [File a GitHub issue](https://github.com/Azure/azure-functions-golang-worker/issues/new/choose)
    and describe your scenario.
