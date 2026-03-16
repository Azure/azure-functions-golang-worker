package main

import (
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

func TimerHandler(timer bindings.TimerInfo) {
	log.Printf("Timer trigger executed")
	log.Printf("Schedule status - Last: %s, Next: %s", timer.ScheduleStatus.Last, timer.ScheduleStatus.Next)
	log.Printf("Is past due: %v", timer.IsPastDue)
}

func main() {
	app := sdk.FunctionApp()

	app.Timer("scheduledTask", TimerHandler).
		Schedule("*/10 * * * * *")

	worker.Start(app)
}
