package function

func hello() (string, error) {
	return "Hello λ!", nil
}

func main() {
	// Make the handler available for Remote Procedure Call by AWS Lambda
	app := main.SetupWorker()
	app.registerCosmos(hello, "foo", "bar")
}
