FROM mcr.microsoft.com/azure-functions/dotnet:4

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true \
    FUNCTIONS_WORKER_RUNTIME=golang \
    AzureWebJobsStorage="<your-storage-connection-string>"

# Copy worker config to the golang worker directory for discovery
COPY worker.config.json /azure-functions-host/workers/golang/

# Copy the compiled app and host config to wwwroot
COPY app /home/site/wwwroot/
COPY host.json /home/site/wwwroot/
