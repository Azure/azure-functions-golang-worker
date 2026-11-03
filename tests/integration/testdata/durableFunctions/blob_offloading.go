package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/azure/azure-functions-golang-worker/middleware/durabletask"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
)

const blobPayloadConnectionStringEnv = "DURABLE_TEST_BLOB_CONNECTION_STRING"

type blobPayloadStore struct {
	*payload.AzureBlobStore
	options *api.LargePayloadOptions

	mu               sync.Mutex
	tokenCount       int
	nativeTokenCount int
}

type largePayload struct {
	Value     string `json:"value"`
	Continued bool   `json:"continued,omitempty"`
}

type inlinePayload struct {
	Value string `json:"value"`
}

type largePayloadRoundTrip struct {
	Input            largePayload  `json:"input"`
	Activity         largePayload  `json:"activity"`
	SubOrchestration largePayload  `json:"subOrchestration"`
	Event            largePayload  `json:"event"`
	Entity           largePayload  `json:"entity"`
	InlineString     string        `json:"inlineString"`
	InlineObject     inlinePayload `json:"inlineObject"`
	Continued        bool          `json:"continued"`
}

func newBlobPayloadStore() (*blobPayloadStore, error) {
	connectionString := os.Getenv(blobPayloadConnectionStringEnv)
	if connectionString == "" {
		return nil, nil
	}

	store, err := payload.NewAzureBlobStore(payload.AzureBlobStoreOptions{
		ConnectionString:  connectionString,
		Container:         "durable-test-payloads",
		AllowInsecureHTTP: true,
	})
	if err != nil {
		return nil, fmt.Errorf("configure durable test blob payload store: %w", err)
	}
	tracked := &blobPayloadStore{AzureBlobStore: store}
	tracked.options = &api.LargePayloadOptions{
		Store:      tracked,
		Resolver:   tracked,
		TokenStore: tracked,
	}
	return tracked, nil
}

func (s *blobPayloadStore) StoreToken(ctx context.Context, data []byte) (string, error) {
	token, err := s.AzureBlobStore.StoreToken(ctx, data)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.tokenCount++
	if s.AzureBlobStore.IsLargePayloadToken(token) {
		s.nativeTokenCount++
	}
	s.mu.Unlock()
	return token, nil
}

func (s *blobPayloadStore) stats() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenCount, s.nativeTokenCount
}

func registerBlobOffloading(app *sdk.App, durable *durabletask.Durable, store *blobPayloadStore) {
	durable.Orchestrator("BlobPayloadRoundTrip", BlobPayloadRoundTrip)
	durable.Orchestrator("BlobPayloadChild", BlobPayloadChild)
	durable.Activity("BlobPayloadEcho", BlobPayloadEcho)
	durable.Activity("BlobInlineStringEcho", BlobInlineStringEcho)
	durable.Activity("BlobInlineObjectEcho", BlobInlineObjectEcho)
	durable.Entity("payload", BlobPayloadEntity)

	app.HTTP("StartBlobPayloadRoundTrip", func(w http.ResponseWriter, r *http.Request) {
		client, ok := durabletask.ClientFromContext(r.Context())
		if !ok {
			http.Error(w, "durable client unavailable", http.StatusInternalServerError)
			return
		}
		var input largePayload
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, "invalid payload: "+err.Error(), http.StatusBadRequest)
			return
		}
		id, err := client.ScheduleNewOrchestration(r.Context(), "BlobPayloadRoundTrip", api.WithInput(input))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"id": string(id)})
	}, sdk.WithMethods("post"), sdk.WithRoute("blob-offloading/start"), sdk.WithAuth("anonymous"), durabletask.ClientInput())

	app.HTTP("RaiseBlobPayloadEvent", func(w http.ResponseWriter, r *http.Request) {
		client, ok := durabletask.ClientFromContext(r.Context())
		if !ok {
			http.Error(w, "durable client unavailable", http.StatusInternalServerError)
			return
		}
		var event largePayload
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, "invalid payload: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := client.RaiseEvent(r.Context(), api.InstanceID(instanceIDFromPath(r, "blob-offloading")),
			"BlobPayloadEvent", api.WithEventPayload(event)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}, sdk.WithMethods("post"), sdk.WithRoute("blob-offloading/{id}/event"), sdk.WithAuth("anonymous"), durabletask.ClientInput())

	app.HTTP("BlobPayloadTokenStats", func(w http.ResponseWriter, _ *http.Request) {
		count, nativeCount := store.stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			TokenCount       int `json:"tokenCount"`
			NativeTokenCount int `json:"nativeTokenCount"`
		}{TokenCount: count, NativeTokenCount: nativeCount})
	}, sdk.WithMethods("get"), sdk.WithRoute("blob-offloading/tokens"), sdk.WithAuth("anonymous"))
}

func BlobPayloadRoundTrip(ctx *task.OrchestrationContext) (any, error) {
	var input largePayload
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	if !input.Continued {
		ctx.ContinueAsNew(largePayload{Value: input.Value, Continued: true})
		return nil, nil
	}
	if err := ctx.SetCustomStatusValue(input); err != nil {
		return nil, err
	}

	const inlineString = "small inline string"
	var inlineStringResult string
	if err := ctx.CallActivity("BlobInlineStringEcho", task.WithActivityInput(inlineString)).
		Await(&inlineStringResult); err != nil {
		return nil, err
	}
	if inlineStringResult != inlineString {
		return nil, errors.New("inline string activity result did not round-trip")
	}
	inlineObject := inlinePayload{Value: "small inline object"}
	var inlineObjectResult inlinePayload
	if err := ctx.CallActivity("BlobInlineObjectEcho", task.WithActivityInput(inlineObject)).
		Await(&inlineObjectResult); err != nil {
		return nil, err
	}
	if inlineObjectResult != inlineObject {
		return nil, errors.New("inline object activity result did not round-trip")
	}

	var activityResult largePayload
	if err := ctx.CallActivity("BlobPayloadEcho", task.WithActivityInput(input)).Await(&activityResult); err != nil {
		return nil, err
	}
	var childResult largePayload
	if err := ctx.CallSubOrchestrator("BlobPayloadChild", task.WithSubOrchestratorInput(activityResult)).
		Await(&childResult); err != nil {
		return nil, err
	}
	var event largePayload
	if err := ctx.WaitForSingleEvent("BlobPayloadEvent", approvalTimeout).Await(&event); err != nil {
		return nil, err
	}

	entityID := api.NewEntityID("payload", string(ctx.ID))
	var entityResult largePayload
	if err := ctx.CallEntity(entityID, "set", task.WithEntityInput(event)).Await(&entityResult); err != nil {
		return nil, err
	}
	var entityState largePayload
	if err := ctx.CallEntity(entityID, "get").Await(&entityState); err != nil {
		return nil, err
	}
	if entityResult != entityState {
		return nil, errors.New("entity operation result differs from persisted state")
	}

	return largePayloadRoundTrip{
		Input:            input,
		Activity:         activityResult,
		SubOrchestration: childResult,
		Event:            event,
		Entity:           entityState,
		InlineString:     inlineStringResult,
		InlineObject:     inlineObjectResult,
		Continued:        input.Continued,
	}, nil
}

func BlobPayloadChild(ctx *task.OrchestrationContext) (any, error) {
	var input largePayload
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return input, nil
}

func BlobPayloadEcho(_ context.Context, input largePayload) (largePayload, error) {
	return input, nil
}

func BlobInlineStringEcho(_ context.Context, input string) (string, error) {
	return input, nil
}

func BlobInlineObjectEcho(_ context.Context, input inlinePayload) (inlinePayload, error) {
	return input, nil
}

func BlobPayloadEntity(ctx *durabletask.EntityContext) (any, error) {
	var state largePayload
	if ctx.HasState() {
		if err := ctx.GetState(&state); err != nil {
			return nil, err
		}
	}
	switch ctx.OperationName {
	case "set":
		if err := ctx.GetInput(&state); err != nil {
			return nil, err
		}
		if err := ctx.SetState(state); err != nil {
			return nil, err
		}
		return state, nil
	case "get":
		return state, nil
	default:
		return nil, fmt.Errorf("unknown payload entity operation %q", ctx.OperationName)
	}
}
