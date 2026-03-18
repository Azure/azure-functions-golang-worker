package sdk

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// --- FunctionApp tests ---

func TestFunctionApp(t *testing.T) {
	app := FunctionApp()
	if app == nil {
		t.Fatal("expected non-nil App")
	}
	if app.RegisteredFunctions == nil {
		t.Fatal("expected non-nil RegisteredFunctions")
	}
}

// --- HTTP builder tests ---

func TestHTTP_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := func(req *http.Request) string { return "hello" }

	builder := app.HTTP("hello", handler)
	if builder == nil {
		t.Fatal("expected non-nil builder")
	}

	// Verify function was registered
	count := 0
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
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
		// First binding should be httpTrigger
		if rf.RawBindings[0].Type != "httpTrigger" {
			t.Errorf("expected first binding type %q, got %q", "httpTrigger", rf.RawBindings[0].Type)
		}
		// Second binding should be http output ($return)
		if rf.RawBindings[1].Name != "$return" {
			t.Errorf("expected second binding name %q, got %q", "$return", rf.RawBindings[1].Name)
		}
		return true
	})

	if count != 1 {
		t.Errorf("expected 1 registered function, got %d", count)
	}
}

func TestHTTP_Methods(t *testing.T) {
	app := FunctionApp()
	handler := func(req *http.Request) string { return "ok" }

	app.HTTP("getonly", handler).Methods("GET")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
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
	handler := func(req *http.Request) string { return "ok" }

	app.HTTP("secure", handler).Auth("function")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.HTTPBinding.AuthLevel != "function" {
			t.Errorf("expected auth level %q, got %q", "function", binding.HTTPBinding.AuthLevel)
		}
		return true
	})
}

func TestHTTP_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := func(req *http.Request) string { return "ok" }

	// Test method chaining
	app.HTTP("chained", handler).
		Methods("POST", "PUT").
		Auth("admin")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
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

func TestHTTP_BlobInput(t *testing.T) {
	app := FunctionApp()
	handler := func(req *http.Request, data []byte) string { return "ok" }

	app.HTTP("withblob", handler).
		BlobInput("inputBlob", "container/{name}", "AzureWebJobsStorage")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		// Should have: httpTrigger, $return, blobInput
		if len(rf.RawBindings) != 3 {
			t.Errorf("expected 3 bindings, got %d", len(rf.RawBindings))
		}
		blobBinding := rf.RawBindings[2]
		if blobBinding.Name != "inputBlob" {
			t.Errorf("expected blob binding name %q, got %q", "inputBlob", blobBinding.Name)
		}
		if blobBinding.Direction != "in" {
			t.Errorf("expected direction %q, got %q", "in", blobBinding.Direction)
		}
		return true
	})
}

func TestHTTP_BlobOutput(t *testing.T) {
	app := FunctionApp()
	handler := func(req *http.Request) string { return "ok" }

	app.HTTP("withblobout", handler).
		BlobOutput("outputBlob", "output/{name}", "AzureWebJobsStorage")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		// Should have: httpTrigger, $return, blobOutput
		if len(rf.RawBindings) != 3 {
			t.Errorf("expected 3 bindings, got %d", len(rf.RawBindings))
		}
		blobBinding := rf.RawBindings[2]
		if blobBinding.Name != "outputBlob" {
			t.Errorf("expected blob binding name %q, got %q", "outputBlob", blobBinding.Name)
		}
		if blobBinding.Direction != "out" {
			t.Errorf("expected direction %q, got %q", "out", blobBinding.Direction)
		}
		return true
	})
}

// --- CosmosDB builder tests ---

func TestCosmosDB_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := func(docs string) {}

	builder := app.CosmosDB("cosmosFunc", handler)
	if builder == nil {
		t.Fatal("expected non-nil builder")
	}

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		if rf.RawBindings[0].Type != "cosmosDBTrigger" {
			t.Errorf("expected type %q, got %q", "cosmosDBTrigger", rf.RawBindings[0].Type)
		}
		return true
	})
}

func TestCosmosDB_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := func(docs string) {}

	app.CosmosDB("cosmosFull", handler).
		Database("mydb").
		Container("mycontainer").
		Connection("CosmosDBConnection")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
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

// --- Blob builder tests ---

func TestBlob_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := func(data []byte) {}

	builder := app.Blob("blobFunc", handler)
	if builder == nil {
		t.Fatal("expected non-nil builder")
	}

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		if rf.RawBindings[0].Type != "blobTrigger" {
			t.Errorf("expected type %q, got %q", "blobTrigger", rf.RawBindings[0].Type)
		}
		return true
	})
}

func TestBlob_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := func(data []byte) {}

	app.Blob("blobFull", handler).
		Path("mycontainer/{name}").
		Connection("AzureWebJobsStorage")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.BlobBinding == nil {
			t.Fatal("expected BlobBinding")
		}
		if binding.BlobBinding.Path != "mycontainer/{name}" {
			t.Errorf("expected path %q, got %q", "mycontainer/{name}", binding.BlobBinding.Path)
		}
		if binding.BlobBinding.Connection != "AzureWebJobsStorage" {
			t.Errorf("expected connection %q, got %q", "AzureWebJobsStorage", binding.BlobBinding.Connection)
		}
		return true
	})
}

func TestBlob_BlobOutput(t *testing.T) {
	app := FunctionApp()
	handler := func(data []byte) {}

	app.Blob("blobWithOutput", handler).
		Path("input/{name}").
		Connection("AzureWebJobsStorage").
		BlobOutput("outputBlob", "output/{name}", "AzureWebJobsStorage")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		if len(rf.RawBindings) != 2 {
			t.Errorf("expected 2 bindings, got %d", len(rf.RawBindings))
		}
		outBinding := rf.RawBindings[1]
		if outBinding.Name != "outputBlob" {
			t.Errorf("expected output binding name %q, got %q", "outputBlob", outBinding.Name)
		}
		if outBinding.Direction != "out" {
			t.Errorf("expected direction %q, got %q", "out", outBinding.Direction)
		}
		return true
	})
}

// --- EventGrid builder tests ---

func TestEventGrid_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := func(event string) {}

	builder := app.EventGrid("eventFunc", handler)
	if builder == nil {
		t.Fatal("expected non-nil builder")
	}

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		if rf.RawBindings[0].Type != "eventGridTrigger" {
			t.Errorf("expected type %q, got %q", "eventGridTrigger", rf.RawBindings[0].Type)
		}
		return true
	})
}

func TestEventGrid_WithOutput(t *testing.T) {
	app := FunctionApp()
	handler := func(event string) {}

	app.EventGrid("eventWithOutput", handler).
		EventGridOutput("outputEvent", "https://topic.endpoint", "TopicKey")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		if len(rf.RawBindings) != 2 {
			t.Errorf("expected 2 bindings, got %d", len(rf.RawBindings))
		}
		outBinding := rf.RawBindings[1]
		if outBinding.Name != "outputEvent" {
			t.Errorf("expected output binding name %q, got %q", "outputEvent", outBinding.Name)
		}
		if outBinding.Type != "eventGrid" {
			t.Errorf("expected type %q, got %q", "eventGrid", outBinding.Type)
		}
		if outBinding.EventGridBinding == nil {
			t.Fatal("expected EventGridBinding")
		}
		if outBinding.EventGridBinding.TopicEndpointUri != "https://topic.endpoint" {
			t.Errorf("expected TopicEndpointUri %q, got %q", "https://topic.endpoint", outBinding.EventGridBinding.TopicEndpointUri)
		}
		return true
	})
}

// --- ServiceBus Queue builder tests ---

func TestServiceBusQueue_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := func(msg string) {}

	builder := app.ServiceBusQueue("sbQueueFunc", handler)
	if builder == nil {
		t.Fatal("expected non-nil builder")
	}

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		if rf.RawBindings[0].Type != "serviceBusTrigger" {
			t.Errorf("expected type %q, got %q", "serviceBusTrigger", rf.RawBindings[0].Type)
		}
		return true
	})
}

func TestServiceBusQueue_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := func(msg string) {}

	app.ServiceBusQueue("sbQueueFull", handler).
		QueueName("myqueue").
		Connection("ServiceBusConnection").
		Cardinality("one")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
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
		if binding.ServiceBusBinding.Cardinality != "one" {
			t.Errorf("expected cardinality %q, got %q", "one", binding.ServiceBusBinding.Cardinality)
		}
		return true
	})
}

func TestServiceBusQueue_WithOutput(t *testing.T) {
	app := FunctionApp()
	handler := func(msg string) {}

	app.ServiceBusQueue("sbQueueWithOutput", handler).
		QueueName("inputqueue").
		Connection("ServiceBusConnection").
		ServiceBusQueueOutput("outputMsg", "outputqueue", "ServiceBusConnection")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		if len(rf.RawBindings) != 2 {
			t.Errorf("expected 2 bindings, got %d", len(rf.RawBindings))
		}
		outBinding := rf.RawBindings[1]
		if outBinding.Name != "outputMsg" {
			t.Errorf("expected output binding name %q, got %q", "outputMsg", outBinding.Name)
		}
		if outBinding.Type != "serviceBus" {
			t.Errorf("expected type %q, got %q", "serviceBus", outBinding.Type)
		}
		if outBinding.Direction != "out" {
			t.Errorf("expected direction %q, got %q", "out", outBinding.Direction)
		}
		return true
	})
}

// --- ServiceBus Topic builder tests ---

func TestServiceBusTopic_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := func(msg string) {}

	builder := app.ServiceBusTopic("sbTopicFunc", handler)
	if builder == nil {
		t.Fatal("expected non-nil builder")
	}

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		if rf.RawBindings[0].Type != "serviceBusTrigger" {
			t.Errorf("expected type %q, got %q", "serviceBusTrigger", rf.RawBindings[0].Type)
		}
		return true
	})
}

func TestServiceBusTopic_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := func(msg string) {}

	app.ServiceBusTopic("sbTopicFull", handler).
		TopicName("mytopic").
		SubscriptionName("mysub").
		Connection("ServiceBusConnection").
		Cardinality("one")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
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
		if binding.ServiceBusBinding.Connection != "ServiceBusConnection" {
			t.Errorf("expected connection %q, got %q", "ServiceBusConnection", binding.ServiceBusBinding.Connection)
		}
		if binding.ServiceBusBinding.Cardinality != "one" {
			t.Errorf("expected cardinality %q, got %q", "one", binding.ServiceBusBinding.Cardinality)
		}
		return true
	})
}

func TestServiceBusTopic_WithOutput(t *testing.T) {
	app := FunctionApp()
	handler := func(msg string) {}

	app.ServiceBusTopic("sbTopicWithOutput", handler).
		TopicName("inputtopic").
		SubscriptionName("mysub").
		Connection("ServiceBusConnection").
		ServiceBusTopicOutput("outputMsg", "outputtopic", "ServiceBusConnection")

	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*RegisteredFunction)
		if len(rf.RawBindings) != 2 {
			t.Errorf("expected 2 bindings, got %d", len(rf.RawBindings))
		}
		outBinding := rf.RawBindings[1]
		if outBinding.Name != "outputMsg" {
			t.Errorf("expected output binding name %q, got %q", "outputMsg", outBinding.Name)
		}
		if outBinding.Type != "serviceBus" {
			t.Errorf("expected type %q, got %q", "serviceBus", outBinding.Type)
		}
		if outBinding.Direction != "out" {
			t.Errorf("expected direction %q, got %q", "out", outBinding.Direction)
		}
		if outBinding.ServiceBusBinding == nil {
			t.Fatal("expected ServiceBusBinding")
		}
		if outBinding.ServiceBusBinding.TopicName != "outputtopic" {
			t.Errorf("expected topicName %q, got %q", "outputtopic", outBinding.ServiceBusBinding.TopicName)
		}
		return true
	})
}

// --- RegisterFunction tests ---

func TestRegisterFunction_HTTP(t *testing.T) {
	app := FunctionApp()
	handler := func(req *http.Request) string { return "ok" }

	rf := app.HTTP("test", handler).rf

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
	handler1 := func() string { return "one" }
	handler2 := func() string { return "two" }

	app.HTTP("func1", handler1)
	app.HTTP("func2", handler2)

	count := 0
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	if count != 2 {
		t.Errorf("expected 2 registered functions, got %d", count)
	}
}

// --- GetFunctionName tests ---

func TestGetFunctionName(t *testing.T) {
	handler := func() {}
	name := GetFunctionName(handler)

	// Should strip package path, leaving just the function name suffix
	if name == "" {
		t.Error("expected non-empty function name")
	}
	// The function is an anonymous closure, so the name will contain "func"
	// but the important thing is it doesn't contain the full package path with dots
}

func myNamedHandler() string { return "named" }

func TestGetFunctionName_NamedFunction(t *testing.T) {
	name := GetFunctionName(myNamedHandler)

	if name != "myNamedHandler" {
		t.Errorf("expected %q, got %q", "myNamedHandler", name)
	}
}

// --- HashFunctionID tests ---

func TestHashFunctionID(t *testing.T) {
	rf := RegisteredFunction{
		FuncName: "testFunc",
	}

	id, err := HashFunctionID(rf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id == "" {
		t.Error("expected non-empty hash")
	}

	// SHA256 produces 64 hex characters
	if len(id) != 64 {
		t.Errorf("expected 64-char hex hash, got %d chars: %s", len(id), id)
	}
}

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
	handler := func() {}

	delay := 5 * time.Second
	rf := app.HTTP("retryFunc", handler).rf
	rf.WithRetry(&RetryOptions{
		MaxRetryCount: 3,
		DelayInterval: &delay,
		Strategy:      ExponentialBackoff,
	})

	if rf.Retry == nil {
		t.Fatal("expected retry options")
	}
	if rf.Retry.MaxRetryCount != 3 {
		t.Errorf("expected MaxRetryCount 3, got %d", rf.Retry.MaxRetryCount)
	}
	if rf.Retry.Strategy != ExponentialBackoff {
		t.Errorf("expected ExponentialBackoff, got %v", rf.Retry.Strategy)
	}
}

// --- Mixed trigger types test ---

func TestMixedTriggerTypes(t *testing.T) {
	app := FunctionApp()

	app.HTTP("httpFunc", func(req *http.Request) string { return "http" })
	app.CosmosDB("cosmosFunc", func(docs string) {}).Database("db").Container("container")
	app.Blob("blobFunc", func(data []byte) {}).Path("container/{name}")
	app.EventGrid("eventFunc", func(event string) {})
	app.ServiceBusQueue("sbFunc", func(msg string) {}).QueueName("myqueue").Connection("SBConn")

	count := 0
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	if count != 5 {
		t.Errorf("expected 5 registered functions, got %d", count)
	}
}

// --- RegisterFunction with context ---

func TestHTTP_FunctionWithContext(t *testing.T) {
	app := FunctionApp()
	handler := func(ctx context.Context, req *http.Request) string { return "with context" }

	app.HTTP("ctxFunc", handler)

	count := 0
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count != 1 {
		t.Errorf("expected 1 registered function, got %d", count)
	}
}
