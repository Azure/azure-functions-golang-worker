# Getting started

Install the required tools, create a Go function application, and run it
locally with Azure Functions Core Tools.

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

You can create the required Azure resources using any supported provisioning
tool, including:

- The [Azure portal](https://portal.azure.com/)
- The Azure CLI
- [Bicep](https://learn.microsoft.com/azure/azure-resource-manager/bicep/)
- Azure Resource Manager (ARM) templates
- Terraform or another infrastructure-as-code tool

The following Azure CLI example creates a resource group, storage account, and
Function App. Replace the placeholder values before running the commands.

Sign in and select the Azure subscription:

```bash
az login
az account set --subscription <SUBSCRIPTION_ID>
```

Create a resource group:

```bash
az group create \
  --name <RESOURCE_GROUP> \
  --location <LOCATION>
```

Create the storage account used by Azure Functions:

```bash
# The name must be globally unique, contain 3-24 characters, and use only
# lowercase letters and numbers.
az storage account create \
  --name <STORAGE_ACCOUNT_NAME> \
  --resource-group <RESOURCE_GROUP> \
  --location <LOCATION> \
  --sku Standard_LRS \
  --kind StorageV2 \
  --allow-blob-public-access false \
  --min-tls-version TLS1_2
```

Create the Go Function App on Flex Consumption:

```bash
# The Function App name must be globally unique.
az functionapp create \
  --name <FUNCTION_APP_NAME> \
  --resource-group <RESOURCE_GROUP> \
  --storage-account <STORAGE_ACCOUNT_NAME> \
  --flexconsumption-location <LOCATION> \
  --runtime golang \
  --runtime-version 1.24 \
  --functions-version 4 \
  --https-only true
```

After creating the Function App, continue developing and testing locally. The
Azure resource is needed only when you are ready to
[deploy the application](guides/deployment.md).
