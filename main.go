package main

import (
	"github.com/azure/azure-functions-golang-worker/functions"
)

type MyStruct struct {
	string1 string
	string2 string
}

func hello(myStruct MyStruct) (string, MyStruct) {
	myString := "Hello"
	myStruct.string1 = myString
	myStruct.string2 = myString + "2"
	return myString, myStruct
}

func main() {
	// Create the app/handler
	app := functions.FunctionApp()

	// Register function(s)
	app.RegisterBlobFunction(hello)

	// Start the worker
	app.StartWorkerServer()
}
