# Deployment

Deploy a Go function application with Azure Functions Core Tools, the Azure
CLI, GitHub Actions, or Azure Pipelines.

These instructions deploy application code to an existing Function App. Create
the Azure resources before deploying.

## Prepare the application

Run deployment commands from the function project root. This is the directory
that contains `host.json` and `go.mod`.

Azure Functions Core Tools builds Go projects automatically when publishing or
packaging them. To create a reusable deployment package without deploying it,
run:

```bash
func pack --output functionapp.zip
```

The resulting ZIP contains the compiled application and generated worker
assets. When creating or inspecting a deployment package, ensure that
`host.json` is at the root of the ZIP rather than inside a parent directory.

Use `--no-build` only when the application has already been built:

```bash
func pack --no-build --output functionapp.zip
```

## Choose a deployment method

| Method | Best suited for |
| --- | --- |
| [Core Tools publish](#publish-with-core-tools) | Local development and manual deployments |
| [Azure CLI ZIP deployment](#deploy-a-zip-with-the-azure-cli) | Scripts and deployment systems that already use the Azure CLI |
| [GitHub Actions](#deploy-with-github-actions) | Continuous deployment from a GitHub repository |
| [Azure Pipelines](#deploy-with-azure-pipelines) | Continuous deployment through Azure DevOps |

## Publish with Core Tools

Sign in to Azure and publish the current project:

```bash
az login
func azure functionapp publish <FUNCTION_APP_NAME>
```

Core Tools builds, packages, and deploys the application. To deploy an
application that was built previously, add `--no-build`:

```bash
func azure functionapp publish <FUNCTION_APP_NAME> --no-build
```

Local settings are not published by default. If the settings in
`local.settings.json` are appropriate for the Azure environment, publish them
explicitly:

```bash
func azure functionapp publish <FUNCTION_APP_NAME> \
  --publish-local-settings
```

!!! warning
    Review `local.settings.json` before publishing it. It commonly contains
    secrets and local emulator connection strings that should not be used in
    Azure.

Use `--subscription` when the Function App is not in the active Azure
subscription:

```bash
func azure functionapp publish <FUNCTION_APP_NAME> \
  --subscription <SUBSCRIPTION_ID>
```

## Deploy a ZIP with the Azure CLI

First create the deployment package:

```bash
func pack --output functionapp.zip
```

Then deploy it with the Azure CLI:

```bash
az functionapp deployment source config-zip \
  --resource-group <RESOURCE_GROUP> \
  --name <FUNCTION_APP_NAME> \
  --src functionapp.zip
```

This approach is useful when authentication and resource selection are already
managed through Azure CLI commands:

```bash
az login
az account set --subscription <SUBSCRIPTION_ID>
```

Do not enable a remote build for this package. `func pack` has already compiled
the Go application and assembled the deployment artifact.

## Deploy with GitHub Actions

Use OpenID Connect (OIDC) authentication so the workflow does not store a
long-lived Azure credential. Configure a federated identity for the GitHub
repository and provide these repository secrets:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_FUNCTIONAPP_NAME`

The following workflow builds a package and deploys it whenever `main` is
updated:

```yaml
name: Deploy function app

on:
  push:
    branches:
      - main
  workflow_dispatch:

permissions:
  contents: read
  id-token: write

jobs:
  deploy:
    runs-on: ubuntu-latest

    steps:
      - name: Check out the repository
        uses: actions/checkout@v6

      - name: Set up Go
        uses: actions/setup-go@v7
        with:
          go-version: "1.24.x"
          cache: true

      - name: Install Azure Functions Core Tools
        run: npm install --global azure-functions-core-tools@4 --unsafe-perm true

      - name: Build deployment package
        run: func pack --output functionapp.zip

      - name: Sign in to Azure
        uses: azure/login@v3
        with:
          client-id: ${{ secrets.AZURE_CLIENT_ID }}
          tenant-id: ${{ secrets.AZURE_TENANT_ID }}
          subscription-id: ${{ secrets.AZURE_SUBSCRIPTION_ID }}

      - name: Deploy function app
        uses: Azure/functions-action@v1
        with:
          app-name: ${{ secrets.AZURE_FUNCTIONAPP_NAME }}
          package: functionapp.zip
```

For production workflows, pin third-party actions to full commit SHAs and use a
protected GitHub environment for deployment approvals.

## Deploy with Azure Pipelines

Create an Azure Resource Manager service connection in the Azure DevOps
project. The example below builds the same ZIP package and deploys it to a Linux
Function App:

```yaml
trigger:
  branches:
    include:
      - main

pool:
  vmImage: ubuntu-latest

variables:
  functionAppName: <FUNCTION_APP_NAME>
  azureServiceConnection: <AZURE_SERVICE_CONNECTION>
  packagePath: $(Build.ArtifactStagingDirectory)/functionapp.zip

steps:
  - task: GoTool@0
    displayName: Use Go 1.24
    inputs:
      version: "1.24"

  - script: npm install --global azure-functions-core-tools@4 --unsafe-perm true
    displayName: Install Azure Functions Core Tools

  - script: func pack --output "$(packagePath)"
    displayName: Build deployment package

  - task: AzureFunctionApp@2
    displayName: Deploy function app
    inputs:
      connectedServiceNameARM: $(azureServiceConnection)
      appType: functionAppLinux
      appName: $(functionAppName)
      package: $(packagePath)
      deploymentMethod: zipDeploy
```

For a Flex Consumption app, set `isFlexConsumption: true` on
`AzureFunctionApp@2` and omit `deploymentMethod`.

## Related documentation

- [Azure Functions Core Tools](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
- [ZIP push deployment for Azure Functions](https://learn.microsoft.com/azure/azure-functions/deployment-zip-push)
- [Deploy Azure Functions with GitHub Actions](https://learn.microsoft.com/azure/azure-functions/functions-how-to-github-actions)
- [Deploy Azure Functions with Azure Pipelines](https://learn.microsoft.com/azure/azure-functions/functions-how-to-azure-devops)
- [`AzureFunctionApp@2` task reference](https://learn.microsoft.com/azure/devops/pipelines/tasks/reference/azure-function-app-v2)
