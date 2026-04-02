package main

import (
	"context"
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

func TimerHandler(ctx context.Context, timer bindings.TimerInfo) error {
	log.Printf("Timer trigger executed")
	log.Printf("Schedule status - Last: %s, Next: %s", timer.ScheduleStatus.Last, timer.ScheduleStatus.Next)
	log.Printf("Is past due: %v", timer.IsPastDue)
	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.Timer("scheduledTask", TimerHandler,
		sdk.WithSchedule("*/10 * * * * *"),
	)

	worker.Start(app)
}
