# Getting started

Install the required tools and create the Azure resources for a Go function
application.

## Prerequisites

Before creating your first function, install:

- [Go 1.24 or later](https://go.dev/dl/)
- [Azure Functions Core Tools](https://www.npmjs.com/package/azure-functions-core-tools/v/4.12.0)
  version 4.12.0 or later, which includes Go worker support
- [Azure CLI 2.87.0 or later](https://learn.microsoft.com/cli/azure/install-azure-cli)
  if you want to create and manage Azure resources from the command line

Install Azure Functions Core Tools using npm:

```bash
npm install --global azure-functions-core-tools@4 --unsafe-perm true
```

Confirm that both tools are available:

```bash
go version
func --version
az version
```

## Create a Function App in Azure

Go support is currently in preview and uses the native Go runtime on the
[Flex Consumption plan](https://learn.microsoft.com/azure/azure-functions/flex-consumption-plan).

The simplest way to create a Function App is through the
[Azure portal](https://portal.azure.com/), which automates the supporting
resource and role assignment setup.

You can also provision the Function App and its dependencies using:

- The Azure CLI
- [Bicep](https://learn.microsoft.com/azure/azure-resource-manager/bicep/)
- Azure Resource Manager (ARM) templates
- Terraform or another infrastructure-as-code tool

After creating the Function App, continue developing and testing locally. The
Azure resource is needed only when you are ready to
[deploy the application](guides/deployment.md).
