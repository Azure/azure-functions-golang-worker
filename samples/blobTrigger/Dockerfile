FROM mcr.microsoft.com/azure-functions/dotnet:4

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true \
    AzureWebJobsStorage="<your-storage-connection-string>"

# Copy worker config to the golang worker directory for discovery
COPY worker.config.json /azure-functions-host/workers/golang/

# Copy the proxy binary, compiled app, and host config to wwwroot
COPY bin/proxy /home/site/wwwroot/
COPY bin/app /home/site/wwwroot/
COPY samples/blobTrigger/host.json /home/site/wwwroot/
