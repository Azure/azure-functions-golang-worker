package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"time"

	"github.com/azure/azure-functions-golang-worker/middleware/durabletask"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
)

// LifecycleWait gives the real extension a short, bounded external-event timer.
func LifecycleWait(ctx *task.OrchestrationContext) (any, error) {
	var seconds int
	if err := ctx.GetInput(&seconds); err != nil {
		return nil, err
	}
	if seconds < 1 || seconds > 120 {
		return nil, errors.New("wait must be between 1 and 120 seconds")
	}
	ctx.SetCustomStatus("waiting for Continue")
	var value string
	err := ctx.WaitForSingleEvent("Continue", time.Duration(seconds)*time.Second).Await(&value)
	if errors.Is(err, task.ErrTaskCanceled) {
		ctx.SetCustomStatus("timed out")
		return "timed out", nil
	}
	if err != nil {
		return nil, err
	}
	ctx.SetCustomStatus("event received")
	return value, nil
}

func BufferedEvent(ctx *task.OrchestrationContext) (any, error) {
	ctx.SetCustomStatus("waiting for Release")
	if err := ctx.WaitForSingleEvent("Release", 60*time.Second).Await(nil); err != nil {
		return nil, err
	}
	var value string
	if err := ctx.WaitForSingleEvent("Continue", 60*time.Second).Await(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// LifecycleManagement exercises the public Go client, including its blocking
// wait APIs. It does not proxy the host's HTTP management API.
func LifecycleManagement(w http.ResponseWriter, r *http.Request) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	var request struct {
		ID      string          `json:"id"`
		Name    string          `json:"name"`
		Input   json.RawMessage `json:"input"`
		Event   string          `json:"event"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	var metadata *api.OrchestrationMetadata
	var err error
	switch path.Base(r.URL.Path) {
	case "schedule":
		var id api.InstanceID
		id, err = client.ScheduleNewOrchestration(ctx, request.Name,
			api.WithInstanceID(api.InstanceID(request.ID)), api.WithInput(request.Input))
		if err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]string{"id": string(id)})
			return
		}
	case "wait-start":
		metadata, err = client.WaitForOrchestrationStart(ctx, api.InstanceID(request.ID))
	case "wait-completion":
		metadata, err = client.WaitForOrchestrationCompletion(ctx, api.InstanceID(request.ID))
	case "fetch":
		metadata, err = client.FetchOrchestrationMetadata(ctx, api.InstanceID(request.ID))
	case "raise":
		err = client.RaiseEvent(ctx, api.InstanceID(request.ID), request.Event,
			api.WithEventPayload(request.Payload))
		if err == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
	default:
		http.Error(w, "unknown management operation", http.StatusBadRequest)
		return
	}
	if errors.Is(err, api.ErrInstanceNotFound) {
		http.Error(w, "api.ErrInstanceNotFound", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = durabletask.WriteStatusResponse(w, metadata)
}

func configureLifecycleProbe(app *sdk.App, durable *durabletask.Durable) {
	durable.Orchestrator("LifecycleWait", LifecycleWait)
	durable.Orchestrator("BufferedEvent", BufferedEvent)
	app.HTTP("LifecycleManagement", LifecycleManagement,
		sdk.WithMethods("post"), sdk.WithRoute("lifecycle/{operation}"),
		sdk.WithAuth("anonymous"), durabletask.ClientInput())
}
