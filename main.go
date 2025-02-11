package main

import (
	"function"
)

func main() {
	// Make the handler available for Remote Procedure Call by AWS Lambda
	app := function.SetupWorker()
	app.registerCosmos(function.hello, "foo", "bar")
}
