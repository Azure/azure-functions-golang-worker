package main

import (
	"time"

	"github.com/azure/azure-functions-golang-worker/functions"
)

func main() {
	// Create the app/handler
	_ = functions.FunctionApp()

	time.Sleep(120 * time.Second)
}
