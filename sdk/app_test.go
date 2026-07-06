package sdk

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// --- FunctionApp tests ---

func TestFunctionApp(t *testing.T) {
	app := FunctionApp()
	if app == nil {
		t.Fatal("expected non-nil App")
	}
	if app.GetRegisteredFunctions() == nil {
		t.Fatal("expected non-nil RegisteredFunctions")
	}
}

// --- HTTP tests ---

func TestHTTP_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	})

	rf := app.HTTP("hello", handler)
	if rf == nil {
		t.Fatal("expected non-nil RegisteredFunction")
	}

	count := 0
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		count++
		rf := value.(*RegisteredFunction)
		if rf.FuncName == "" {
			t.Error("expected non-empty FuncName")
		}
		if rf.FuncId == "" {
			t.Error("expected non-empty FuncId")
		}
		if len(rf.RawBindings) < 2 {
			t.Errorf("expected at least 2 bindings (trigger + $return), got %d", len(rf.RawBindings))
		}
		if rf.RawBindings[0].Type != "httpTrigger" {
			t.Errorf("expected first binding type %q, got %q", "httpTrigger", rf.RawBindings[0].Type)
		}
		if rf.RawBindings[1].Name != "$return" {
			t.Errorf("expected second binding name %q, got %q", "$return", rf.RawBindings[1].Name)
		}
		if rf.TriggerType != "httpTrigger" {
			t.Errorf("expected TriggerType %q, got %q", "httpTrigger", rf.TriggerType)
		}
		return true
	})

	if count != 1 {
		t.Errorf("expected 1 registered function, got %d", count)
	}
}

func TestHTTP_Methods(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {})

	app.HTTP("getonly", handler, WithMethods("GET"))

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.HTTPBinding == nil {
			t.Fatal("expected HTTPBinding")
		}
		if len(binding.HTTPBinding.Methods) != 1 || binding.HTTPBinding.Methods[0] != "GET" {
			t.Errorf("expected methods [GET], got %v", binding.HTTPBinding.Methods)
		}
		return true
	})
}

func TestHTTP_Auth(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {})

	app.HTTP("secure", handler, WithAuth("function"))

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.HTTPBinding.AuthLevel != "function" {
			t.Errorf("expected auth level %q, got %q", "function", binding.HTTPBinding.AuthLevel)
		}
		return true
	})
}

func TestHTTP_MultipleOptions(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {})

	app.HTTP("chained", handler,
		WithMethods("POST", "PUT"),
		WithAuth("admin"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if len(binding.HTTPBinding.Methods) != 2 {
			t.Errorf("expected 2 methods, got %d", len(binding.HTTPBinding.Methods))
		}
		if binding.HTTPBinding.AuthLevel != "admin" {
			t.Errorf("expected auth level %q, got %q", "admin", binding.HTTPBinding.AuthLevel)
		}
		return true
	})
}

// --- Timer tests ---

func TestTimer_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := TimerHandler(func(ctx context.Context, timer bindings.TimerInfo) error {
		return nil
	})

	app.Timer("tick", handler, WithSchedule("0 */5 * * * *"))

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		if rf.RawBindings[0].Type != "timerTrigger" {
			t.Errorf("expected type %q, got %q", "timerTrigger", rf.RawBindings[0].Type)
		}
		if rf.TriggerType != "timerTrigger" {
			t.Errorf("expected TriggerType %q, got %q", "timerTrigger", rf.TriggerType)
		}
		if len(rf.RawBindings) != 1 {
			t.Errorf("expected 1 binding, got %d", len(rf.RawBindings))
		}
		return true
	})
}

// --- CosmosDB tests ---

func TestCosmosDB_Options(t *testing.T) {
	app := FunctionApp()
	handler := CosmosDBHandler(func(ctx context.Context, docs []bindings.CosmosDocument) error {
		return nil
	})

	app.CosmosDB("cosmosFull", handler,
		WithDatabase("mydb"),
		WithContainer("mycontainer"),
		WithConnection("CosmosDBConnection"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.CosmosDBBinding == nil {
			t.Fatal("expected CosmosDBBinding")
		}
		if binding.CosmosDBBinding.DatabaseName != "mydb" {
			t.Errorf("expected database %q, got %q", "mydb", binding.CosmosDBBinding.DatabaseName)
		}
		if binding.CosmosDBBinding.ContainerName != "mycontainer" {
			t.Errorf("expected container %q, got %q", "mycontainer", binding.CosmosDBBinding.ContainerName)
		}
		if binding.CosmosDBBinding.Connection != "CosmosDBConnection" {
			t.Errorf("expected connection %q, got %q", "CosmosDBConnection", binding.CosmosDBBinding.Connection)
		}
		return true
	})
}

// --- EventGrid tests ---

func TestCosmosDB_WithCreateLeaseContainerIfNotExists(t *testing.T) {
	app := FunctionApp()
	handler := CosmosDBHandler(func(ctx context.Context, docs []bindings.CosmosDocument) error {
		return nil
	})

	app.CosmosDB("cosmosAutoLease", handler,
		WithDatabase("mydb"),
		WithContainer("mycontainer"),
		WithConnection("CosmosDBConnection"),
		WithCreateLeaseContainerIfNotExists(true),
	)

	found := false
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.CosmosDBBinding == nil {
			t.Fatal("expected CosmosDBBinding")
		}
		if !binding.CosmosDBBinding.CreateLeaseContainerIfNotExists {
			t.Error("expected CreateLeaseContainerIfNotExists=true after applying option")
		}
		found = true
		return true
	})
	if !found {
		t.Fatal("expected at least one registered function")
	}
}

func TestWithCreateLeaseContainerIfNotExists_NonCosmosTriggerIsNoop(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {})

	// Applying a Cosmos-only option to an HTTP trigger must not panic and
	// must not mutate the HTTP binding.
	app.HTTP("hello", handler, WithCreateLeaseContainerIfNotExists(true))

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		if rf.RawBindings[0].CosmosDBBinding != nil {
			t.Error("expected no CosmosDBBinding on an HTTP trigger")
		}
		return true
	})
}

func TestEventGrid_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := EventGridHandler(func(ctx context.Context, event bindings.EventGridEvent) error {
		return nil
	})

	app.EventGrid("eventFunc", handler)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		if rf.RawBindings[0].Type != "eventGridTrigger" {
			t.Errorf("expected type %q, got %q", "eventGridTrigger", rf.RawBindings[0].Type)
		}
		return true
	})
}

// --- EventHub tests ---

func TestEventHub_Options(t *testing.T) {
	app := FunctionApp()
	handler := EventHubHandler(func(ctx context.Context, event bindings.EventHubMessage) error {
		return nil
	})

	app.EventHub("ehFunc", handler,
		WithEventHubName("myeventhub"),
		WithConnection("EventHubConnection"),
		WithConsumerGroup("$Default"),
		WithCardinality("one"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.EventHubBinding == nil {
			t.Fatal("expected EventHubBinding")
		}
		if binding.EventHubBinding.EventHubName != "myeventhub" {
			t.Errorf("expected eventHubName %q, got %q", "myeventhub", binding.EventHubBinding.EventHubName)
		}
		if binding.EventHubBinding.Connection != "EventHubConnection" {
			t.Errorf("expected connection %q, got %q", "EventHubConnection", binding.EventHubBinding.Connection)
		}
		return true
	})
}

// --- ServiceBus Queue tests ---

func TestServiceBusQueue_Options(t *testing.T) {
	app := FunctionApp()
	handler := ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error {
		return nil
	})

	app.ServiceBusQueue("sbQueueFull", handler,
		WithQueueName("myqueue"),
		WithConnection("ServiceBusConnection"),
		WithCardinality("one"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.ServiceBusBinding == nil {
			t.Fatal("expected ServiceBusBinding")
		}
		if binding.ServiceBusBinding.QueueName != "myqueue" {
			t.Errorf("expected queueName %q, got %q", "myqueue", binding.ServiceBusBinding.QueueName)
		}
		if binding.ServiceBusBinding.Connection != "ServiceBusConnection" {
			t.Errorf("expected connection %q, got %q", "ServiceBusConnection", binding.ServiceBusBinding.Connection)
		}
		return true
	})
}

// --- ServiceBus Topic tests ---

func TestServiceBusTopic_Options(t *testing.T) {
	app := FunctionApp()
	handler := ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error {
		return nil
	})

	app.ServiceBusTopic("sbTopicFull", handler,
		WithTopicName("mytopic"),
		WithSubscriptionName("mysub"),
		WithConnection("ServiceBusConnection"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.ServiceBusBinding == nil {
			t.Fatal("expected ServiceBusBinding")
		}
		if binding.ServiceBusBinding.TopicName != "mytopic" {
			t.Errorf("expected topicName %q, got %q", "mytopic", binding.ServiceBusBinding.TopicName)
		}
		if binding.ServiceBusBinding.SubscriptionName != "mysub" {
			t.Errorf("expected subscriptionName %q, got %q", "mysub", binding.ServiceBusBinding.SubscriptionName)
		}
		return true
	})
}

// --- Blob tests ---

func TestBlob_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := BlobHandler(func(ctx context.Context, data []byte) error {
		return nil
	})

	rf := app.Blob("processBlob", handler)
	if rf == nil {
		t.Fatal("expected non-nil RegisteredFunction")
	}

	count := 0
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		count++
		rf := value.(*RegisteredFunction)
		if rf.FuncName == "" {
			t.Error("expected non-empty FuncName")
		}
		if rf.FuncId == "" {
			t.Error("expected non-empty FuncId")
		}
		if len(rf.RawBindings) != 1 {
			t.Errorf("expected 1 binding (trigger only, no output), got %d", len(rf.RawBindings))
		}
		if rf.RawBindings[0].Type != "blobTrigger" {
			t.Errorf("expected binding type %q, got %q", "blobTrigger", rf.RawBindings[0].Type)
		}
		if rf.TriggerType != "blobTrigger" {
			t.Errorf("expected TriggerType %q, got %q", "blobTrigger", rf.TriggerType)
		}
		return true
	})

	if count != 1 {
		t.Errorf("expected 1 registered function, got %d", count)
	}
}

func TestBlob_WithConnection(t *testing.T) {
	app := FunctionApp()
	handler := BlobHandler(func(ctx context.Context, data []byte) error {
		return nil
	})

	app.Blob("blobConn", handler,
		WithPath("samples-workitems/{name}"),
		WithConnection("AzureWebJobsStorage"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.BlobBinding == nil {
			t.Fatal("expected BlobBinding")
		}
		if binding.BlobBinding.Path != "samples-workitems/{name}" {
			t.Errorf("expected path %q, got %q", "samples-workitems/{name}", binding.BlobBinding.Path)
		}
		if binding.BlobBinding.Connection != "AzureWebJobsStorage" {
			t.Errorf("expected connection %q, got %q", "AzureWebJobsStorage", binding.BlobBinding.Connection)
		}
		return true
	})
}

func TestBlob_NoOutputBinding(t *testing.T) {
	app := FunctionApp()
	handler := BlobHandler(func(ctx context.Context, data []byte) error {
		return nil
	})

	rf := app.Blob("blobNoOutput", handler)

	if len(rf.RawBindings) != 1 {
		t.Errorf("blob trigger should have 1 binding (no $return output), got %d", len(rf.RawBindings))
	}
	if rf.RawBindings[0].Name == "$return" {
		t.Error("blob trigger should not have a $return output binding")
	}
}

func TestBlob_DefaultBindingValues(t *testing.T) {
	app := FunctionApp()
	handler := BlobHandler(func(ctx context.Context, data []byte) error {
		return nil
	})

	rf := app.Blob("blobDefaults", handler)

	binding := rf.RawBindings[0]
	if binding.BlobBinding == nil {
		t.Fatal("expected BlobBinding")
	}
	if binding.BlobBinding.Path != "" {
		t.Errorf("expected empty default path, got %q", binding.BlobBinding.Path)
	}
	if binding.BlobBinding.Connection != "" {
		t.Errorf("expected empty default connection, got %q", binding.BlobBinding.Connection)
	}
	if binding.Direction != "in" {
		t.Errorf("expected direction %q, got %q", "in", binding.Direction)
	}
}

func TestBlob_MismatchedOption_NoOp(t *testing.T) {
	app := FunctionApp()
	handler := BlobHandler(func(ctx context.Context, data []byte) error {
		return nil
	})

	// HTTP-specific options should be no-ops on a blob trigger
	rf := app.Blob("blobMismatch", handler,
		WithMethods("GET"),
		WithAuth("admin"),
		WithConnection("AzureWebJobsStorage"),
	)

	if rf.RawBindings[0].BlobBinding == nil {
		t.Fatal("expected BlobBinding")
	}
	// Connection should still apply (it's a shared option)
	if rf.RawBindings[0].BlobBinding.Connection != "AzureWebJobsStorage" {
		t.Errorf("expected connection %q, got %q", "AzureWebJobsStorage", rf.RawBindings[0].BlobBinding.Connection)
	}
	// HTTP binding should remain nil (HTTP options are no-ops)
	if rf.RawBindings[0].HTTPBinding != nil {
		t.Error("HTTP-specific options should not create an HTTPBinding on a blob trigger")
	}
}

// --- No output bindings tests ---

func TestServiceBusQueue_NoOutputBindings(t *testing.T) {
	app := FunctionApp()
	handler := ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error {
		return nil
	})

	app.ServiceBusQueue("sbQueue", handler,
		WithQueueName("myqueue"),
		WithConnection("ServiceBusConnection"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		if len(rf.RawBindings) != 1 {
			t.Errorf("expected 1 binding (trigger only), got %d", len(rf.RawBindings))
		}
		return true
	})
}

// --- RegisterFunction tests ---

func TestRegisterFunction_HTTP(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {})

	rf := app.HTTP("test", handler)

	if rf.FuncName == "" {
		t.Error("expected non-empty FuncName")
	}
	if rf.FuncId == "" {
		t.Error("expected non-empty FuncId")
	}
	if rf.ScriptFile == "" {
		t.Error("expected non-empty ScriptFile")
	}
	if rf.Func == nil {
		t.Error("expected non-nil Func")
	}
}

func TestRegisterFunction_MultipleFunctions(t *testing.T) {
	app := FunctionApp()
	h1 := HTTPHandler(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("one")) })
	h2 := HTTPHandler(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("two")) })

	app.HTTP("func1", h1)
	app.HTTP("func2", h2)

	count := 0
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		count++
		return true
	})

	if count != 2 {
		t.Errorf("expected 2 registered functions, got %d", count)
	}
}

// --- Mixed trigger types test ---

func TestMixedTriggerTypes(t *testing.T) {
	app := FunctionApp()

	app.HTTP("httpFunc", HTTPHandler(func(w http.ResponseWriter, r *http.Request) {}))
	app.CosmosDB("cosmosFunc", CosmosDBHandler(func(ctx context.Context, docs []bindings.CosmosDocument) error { return nil }),
		WithDatabase("db"), WithContainer("container"))
	app.EventGrid("eventFunc", EventGridHandler(func(ctx context.Context, event bindings.EventGridEvent) error { return nil }))
	app.ServiceBusQueue("sbFunc", ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error { return nil }),
		WithQueueName("myqueue"), WithConnection("SBConn"))
	app.Blob("blobFunc", BlobHandler(func(ctx context.Context, data []byte) error { return nil }),
		WithConnection("AzureWebJobsStorage"))

	count := 0
	triggerTypes := map[string]bool{}
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		count++
		rf := value.(*RegisteredFunction)
		triggerTypes[rf.TriggerType] = true
		return true
	})

	if count != 5 {
		t.Errorf("expected 5 registered functions, got %d", count)
	}
	if !triggerTypes["blobTrigger"] {
		t.Error("expected blobTrigger in registered trigger types")
	}
}

// --- HashFunctionID tests ---

func TestHashFunctionID_Deterministic(t *testing.T) {
	rf := RegisteredFunction{FuncName: "stableFunc"}

	id1, _ := HashFunctionID(rf)
	id2, _ := HashFunctionID(rf)

	if id1 != id2 {
		t.Errorf("hash should be deterministic: %q != %q", id1, id2)
	}
}

func TestHashFunctionID_UniquePerName(t *testing.T) {
	rf1 := RegisteredFunction{FuncName: "funcA"}
	rf2 := RegisteredFunction{FuncName: "funcB"}

	id1, _ := HashFunctionID(rf1)
	id2, _ := HashFunctionID(rf2)

	if id1 == id2 {
		t.Error("different function names should produce different hashes")
	}
}

// --- WithRetry tests ---

func TestWithRetry(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {})

	delay := 5 * time.Second
	rf := app.HTTP("retryFunc", handler,
		WithRetry(&RetryOptions{
			MaxRetryCount: 3,
			DelayInterval: &delay,
			Strategy:      ExponentialBackoff,
		}),
	)

	if rf.Retry == nil {
		t.Fatal("expected retry options")
	}
	if rf.Retry.MaxRetryCount != 3 {
		t.Errorf("expected MaxRetryCount 3, got %d", rf.Retry.MaxRetryCount)
	}
}

// --- Shared option cross-trigger tests ---

func TestWithConnection_MultipleTriggersTypes(t *testing.T) {
	app := FunctionApp()

	// EventHub with WithConnection
	app.EventHub("eh", EventHubHandler(func(ctx context.Context, e bindings.EventHubMessage) error { return nil }),
		WithEventHubName("hub"),
		WithConnection("SharedConn"),
	)

	// ServiceBus Queue with same WithConnection
	app.ServiceBusQueue("sb", ServiceBusHandler(func(ctx context.Context, m bindings.ServiceBusMessage) error { return nil }),
		WithQueueName("q"),
		WithConnection("SharedConn"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		b := rf.RawBindings[0]
		if b.EventHubBinding != nil && b.EventHubBinding.Connection != "SharedConn" {
			t.Errorf("expected EventHub connection %q, got %q", "SharedConn", b.EventHubBinding.Connection)
		}
		if b.ServiceBusBinding != nil && b.ServiceBusBinding.Connection != "SharedConn" {
			t.Errorf("expected ServiceBus connection %q, got %q", "SharedConn", b.ServiceBusBinding.Connection)
		}
		return true
	})
}

// --- Mismatched option no-op tests ---

func TestMismatchedOption_NoOp(t *testing.T) {
	app := FunctionApp()
	handler := TimerHandler(func(ctx context.Context, timer bindings.TimerInfo) error { return nil })

	// WithMethods is HTTP-specific, should be a no-op on a timer trigger
	rf := app.Timer("tick", handler,
		WithSchedule("0 */5 * * * *"),
		WithMethods("GET"),
	)

	if rf.RawBindings[0].TimerBinding == nil {
		t.Fatal("expected TimerBinding")
	}
	if rf.RawBindings[0].TimerBinding.Schedule != "0 */5 * * * *" {
		t.Errorf("expected schedule %q, got %q", "0 */5 * * * *", rf.RawBindings[0].TimerBinding.Schedule)
	}
}

// --- WithRoute test ---

func TestHTTP_WithRoute(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {})

	app.HTTP("routeFunc", handler, WithRoute("custom/path"))

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.HTTPBinding == nil {
			t.Fatal("expected HTTPBinding")
		}
		if binding.HTTPBinding.Route != "custom/path" {
			t.Errorf("expected route %q, got %q", "custom/path", binding.HTTPBinding.Route)
		}
		return true
	})
}

// --- WithIsSessionsEnabled test ---

func TestServiceBusQueue_WithIsSessionsEnabled(t *testing.T) {
	app := FunctionApp()
	handler := ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error { return nil })

	app.ServiceBusQueue("sbSessions", handler,
		WithQueueName("session-queue"),
		WithConnection("SBConn"),
		WithIsSessionsEnabled(true),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.ServiceBusBinding == nil {
			t.Fatal("expected ServiceBusBinding")
		}
		if !binding.ServiceBusBinding.IsSessionsEnabled {
			t.Error("expected IsSessionsEnabled to be true")
		}
		return true
	})
}

// --- WithCardinality on ServiceBus test ---

func TestServiceBusQueue_WithCardinality(t *testing.T) {
	app := FunctionApp()
	handler := ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error { return nil })

	app.ServiceBusQueue("sbCard", handler,
		WithQueueName("card-queue"),
		WithCardinality("many"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.ServiceBusBinding == nil {
			t.Fatal("expected ServiceBusBinding")
		}
		if binding.ServiceBusBinding.Cardinality != "many" {
			t.Errorf("expected cardinality %q, got %q", "many", binding.ServiceBusBinding.Cardinality)
		}
		return true
	})
}

// --- Blob panic tests ---

func TestBlob_PanicOnNonFunction(t *testing.T) {
	app := FunctionApp()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-function handler")
		}
	}()
	app.Blob("bad", "not a function")
}

func TestBlob_PanicOnWrongArgCount(t *testing.T) {
	app := FunctionApp()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for wrong arg count")
		}
	}()
	app.Blob("bad", func() error { return nil })
}

func TestBlob_PanicOnNonContextFirstArg(t *testing.T) {
	app := FunctionApp()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-context first arg")
		}
	}()
	app.Blob("bad", func(s string, data []byte) error { return nil })
}

func TestBlob_PanicOnWrongReturnCount(t *testing.T) {
	app := FunctionApp()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for wrong return count")
		}
	}()
	app.Blob("bad", func(ctx context.Context, data []byte) {})
}

func TestBlob_PanicOnNonErrorReturn(t *testing.T) {
	app := FunctionApp()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-error return type")
		}
	}()
	app.Blob("bad", func(ctx context.Context, data []byte) string { return "" })
}

// --- RegisterClientFactory test ---

func TestRegisterClientFactory_AndRetrieve(t *testing.T) {
	factoryCalled := false
	factory := ClientFactory(func(config map[string]any, triggerMetadata map[string]string) (any, error) {
		factoryCalled = true
		return "mock-client", nil
	})

	RegisterClientFactory("testTrigger", factory)

	got, ok := GetClientFactory("testTrigger")
	if !ok {
		t.Fatal("expected factory to be registered")
	}
	result, err := got(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "mock-client" {
		t.Errorf("expected %q, got %q", "mock-client", result)
	}
	if !factoryCalled {
		t.Error("expected factory to be called")
	}
}

func TestRegisterClientFactory_DuplicatePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for duplicate registration")
		}
	}()
	noop := ClientFactory(func(config map[string]any, triggerMetadata map[string]string) (any, error) {
		return nil, nil
	})
	RegisterClientFactory("dupTrigger", noop)
	RegisterClientFactory("dupTrigger", noop) // should panic
}

// --- GetFunctionName test ---

func TestGetFunctionName(t *testing.T) {
	name := GetFunctionName(TestGetFunctionName)
	if name != "TestGetFunctionName" {
		t.Errorf("expected %q, got %q", "TestGetFunctionName", name)
	}
}

// --- RegisterFunction (exported) test ---

func TestRegisterFunction_Exported(t *testing.T) {
	app := FunctionApp()
	handler := func(ctx context.Context, data []byte) error { return nil }

	trigger := &bindings.BlobTrigger{Name: "blob"}
	rf := app.RegisterFunction("exportedBlob", handler, trigger,
		WithPath("my-container/{name}"),
		WithConnection("AzureWebJobsStorage"),
	)

	if rf.FuncName != "exportedBlob" {
		t.Errorf("expected FuncName %q, got %q", "exportedBlob", rf.FuncName)
	}
	if rf.FuncId == "" {
		t.Error("expected non-empty FuncId")
	}
	if rf.RawBindings[0].BlobBinding == nil {
		t.Fatal("expected BlobBinding")
	}
	if rf.RawBindings[0].BlobBinding.Path != "my-container/{name}" {
		t.Errorf("expected path %q, got %q", "my-container/{name}", rf.RawBindings[0].BlobBinding.Path)
	}
}

// --- TriggerBinding exported helper test ---

func TestTriggerBinding_Empty(t *testing.T) {
	rf := &RegisteredFunction{}
	if rf.TriggerBinding() != nil {
		t.Error("expected nil TriggerBinding for empty RawBindings")
	}
}

// --- SQL tests ---

func TestSQL_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		return nil
	})

	app.SQL("productsChanged", handler)

	count := 0
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		count++
		rf := value.(*RegisteredFunction)
		if rf.FuncName != "productsChanged" {
			t.Errorf("expected func name %q, got %q", "productsChanged", rf.FuncName)
		}
		if rf.TriggerType != string(bindings.SQLTriggerType) {
			t.Errorf("expected trigger type %q, got %q", bindings.SQLTriggerType, rf.TriggerType)
		}
		if len(rf.RawBindings) != 1 {
			t.Errorf("expected 1 binding, got %d", len(rf.RawBindings))
		}
		b := rf.RawBindings[0]
		if b.Type != "sqlTrigger" {
			t.Errorf("expected type %q, got %q", "sqlTrigger", b.Type)
		}
		if b.Direction != "in" {
			t.Errorf("expected direction %q, got %q", "in", b.Direction)
		}
		if b.Name != "changes" {
			t.Errorf("expected default binding name %q, got %q", "changes", b.Name)
		}
		if b.SQLBinding == nil {
			t.Fatal("expected SQLBinding")
		}
		return true
	})
	if count != 1 {
		t.Errorf("expected 1 registered function, got %d", count)
	}
}

func TestSQL_Options(t *testing.T) {
	app := FunctionApp()
	handler := SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		return nil
	})

	app.SQL("productsChanged", handler,
		WithTable("dbo.Products"),
		WithConnection("AzureWebJobsSqlConnectionString"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.SQLBinding == nil {
			t.Fatal("expected SQLBinding")
		}
		if binding.SQLBinding.TableName != "dbo.Products" {
			t.Errorf("expected tableName %q, got %q",
				"dbo.Products", binding.SQLBinding.TableName)
		}
		if binding.SQLBinding.ConnectionStringSetting != "AzureWebJobsSqlConnectionString" {
			t.Errorf("expected connectionStringSetting %q, got %q",
				"AzureWebJobsSqlConnectionString",
				binding.SQLBinding.ConnectionStringSetting)
		}
		return true
	})
}

// TestSQL_WithConnectionPopulatesSQLBinding locks down that the shared
// WithConnection option targets the SQL binding's connectionStringSetting
// field, so SQL users get the same one-option-fits-all surface as the
// CosmosDB / EventHub / ServiceBus / Blob triggers.
func TestSQL_WithConnectionPopulatesSQLBinding(t *testing.T) {
	app := FunctionApp()
	handler := SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		return nil
	})

	app.SQL("productsChanged", handler,
		WithTable("dbo.Products"),
		WithConnection("AzureWebJobsSqlConnectionString"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.SQLBinding == nil {
			t.Fatal("expected SQLBinding")
		}
		if binding.SQLBinding.ConnectionStringSetting != "AzureWebJobsSqlConnectionString" {
			t.Errorf("WithConnection must populate SQL connectionStringSetting; got %q",
				binding.SQLBinding.ConnectionStringSetting)
		}
		return true
	})
}

func TestWithTable_PanicsOnEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty table name")
		}
	}()
	WithTable("")
}

func TestSQL_WithLeasesTable(t *testing.T) {
	app := FunctionApp()
	handler := SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		return nil
	})

	app.SQL("productsChanged", handler,
		WithTable("dbo.Products"),
		WithConnection("AzureWebJobsSqlConnectionString"),
		WithLeasesTable("MyCustomLeases"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.SQLBinding == nil {
			t.Fatal("expected SQLBinding")
		}
		if binding.SQLBinding.LeasesTableName != "MyCustomLeases" {
			t.Errorf("expected leasesTableName %q, got %q",
				"MyCustomLeases", binding.SQLBinding.LeasesTableName)
		}
		return true
	})
}

// --- Queue Storage tests ---

func TestQueue_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := QueueHandler(func(ctx context.Context, msg bindings.QueueMessage) error {
		return nil
	})

	rf := app.Queue("processQueue", handler,
		WithQueueName("myqueue"),
		WithConnection("AzureWebJobsStorage"),
	)

	if rf == nil {
		t.Fatal("expected non-nil RegisteredFunction")
	}
	if rf.FuncName != "processQueue" {
		t.Errorf("expected FuncName %q, got %q", "processQueue", rf.FuncName)
	}
	if rf.TriggerType != "queueTrigger" {
		t.Errorf("expected TriggerType %q, got %q", "queueTrigger", rf.TriggerType)
	}
}

func TestQueue_Options(t *testing.T) {
	app := FunctionApp()
	handler := QueueHandler(func(ctx context.Context, msg bindings.QueueMessage) error {
		return nil
	})

	app.Queue("queueFunc", handler,
		WithQueueName("testqueue"),
		WithConnection("StorageConn"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.QueueBinding == nil {
			t.Fatal("expected QueueBinding")
		}
		if binding.QueueBinding.QueueName != "testqueue" {
			t.Errorf("expected queueName %q, got %q", "testqueue", binding.QueueBinding.QueueName)
		}
		if binding.QueueBinding.Connection != "StorageConn" {
			t.Errorf("expected connection %q, got %q", "StorageConn", binding.QueueBinding.Connection)
		}
		return true
	})
}

func TestQueue_NoOutputBindings(t *testing.T) {
	app := FunctionApp()
	handler := QueueHandler(func(ctx context.Context, msg bindings.QueueMessage) error {
		return nil
	})

	app.Queue("queueNoOutput", handler,
		WithQueueName("q1"),
		WithConnection("Conn"),
	)

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		if len(rf.RawBindings) != 1 {
			t.Errorf("expected 1 binding (trigger only, no $return), got %d", len(rf.RawBindings))
		}
		return true
	})
}
