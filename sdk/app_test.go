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

// --- HTTP builder tests ---

func TestHTTP_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	})

	builder := app.HTTP("hello", handler)
	if builder == nil {
		t.Fatal("expected non-nil builder")
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

	app.HTTP("getonly", handler).Methods("GET")

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.HttpBinding == nil {
			t.Fatal("expected HttpBinding")
		}
		if len(binding.HttpBinding.Methods) != 1 || binding.HttpBinding.Methods[0] != "GET" {
			t.Errorf("expected methods [GET], got %v", binding.HttpBinding.Methods)
		}
		return true
	})
}

func TestHTTP_Auth(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {})

	app.HTTP("secure", handler).Auth("function")

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if binding.HttpBinding.AuthLevel != "function" {
			t.Errorf("expected auth level %q, got %q", "function", binding.HttpBinding.AuthLevel)
		}
		return true
	})
}

func TestHTTP_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := HTTPHandler(func(w http.ResponseWriter, r *http.Request) {})

	app.HTTP("chained", handler).
		Methods("POST", "PUT").
		Auth("admin")

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		binding := rf.RawBindings[0]
		if len(binding.HttpBinding.Methods) != 2 {
			t.Errorf("expected 2 methods, got %d", len(binding.HttpBinding.Methods))
		}
		if binding.HttpBinding.AuthLevel != "admin" {
			t.Errorf("expected auth level %q, got %q", "admin", binding.HttpBinding.AuthLevel)
		}
		return true
	})
}

// --- Timer builder tests ---

func TestTimer_BasicRegistration(t *testing.T) {
	app := FunctionApp()
	handler := TimerHandler(func(ctx context.Context, timer bindings.TimerInfo) error {
		return nil
	})

	app.Timer("tick", handler).Schedule("0 */5 * * * *")

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		if rf.RawBindings[0].Type != "timerTrigger" {
			t.Errorf("expected type %q, got %q", "timerTrigger", rf.RawBindings[0].Type)
		}
		if rf.TriggerType != "timerTrigger" {
			t.Errorf("expected TriggerType %q, got %q", "timerTrigger", rf.TriggerType)
		}
		// Timer should have exactly 1 binding (trigger only, no $return)
		if len(rf.RawBindings) != 1 {
			t.Errorf("expected 1 binding, got %d", len(rf.RawBindings))
		}
		return true
	})
}

// --- CosmosDB builder tests ---

func TestCosmosDB_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := CosmosDBHandler(func(ctx context.Context, docs []bindings.CosmosDocument) error {
		return nil
	})

	app.CosmosDB("cosmosFull", handler).
		Database("mydb").
		Container("mycontainer").
		Connection("CosmosDBConnection")

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

// --- EventGrid builder tests ---

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

// --- EventHub builder tests ---

func TestEventHub_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := EventHubHandler(func(ctx context.Context, event bindings.EventHubMessage) error {
		return nil
	})

	app.EventHub("ehFunc", handler).
		EventHubName("myeventhub").
		Connection("EventHubConnection").
		ConsumerGroup("$Default").
		Cardinality("one")

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

// --- ServiceBus Queue builder tests ---

func TestServiceBusQueue_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error {
		return nil
	})

	app.ServiceBusQueue("sbQueueFull", handler).
		QueueName("myqueue").
		Connection("ServiceBusConnection").
		Cardinality("one")

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

// --- ServiceBus Topic builder tests ---

func TestServiceBusTopic_Chaining(t *testing.T) {
	app := FunctionApp()
	handler := ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error {
		return nil
	})

	app.ServiceBusTopic("sbTopicFull", handler).
		TopicName("mytopic").
		SubscriptionName("mysub").
		Connection("ServiceBusConnection")

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

// --- No output bindings tests ---

func TestServiceBusQueue_NoOutputBindings(t *testing.T) {
	app := FunctionApp()
	handler := ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error {
		return nil
	})

	app.ServiceBusQueue("sbQueue", handler).
		QueueName("myqueue").
		Connection("ServiceBusConnection")

	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*RegisteredFunction)
		// Should only have 1 binding (trigger) — no output bindings
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
	app.CosmosDB("cosmosFunc", CosmosDBHandler(func(ctx context.Context, docs []bindings.CosmosDocument) error { return nil })).Database("db").Container("container")
	app.EventGrid("eventFunc", EventGridHandler(func(ctx context.Context, event bindings.EventGridEvent) error { return nil }))
	app.ServiceBusQueue("sbFunc", ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error { return nil })).QueueName("myqueue").Connection("SBConn")

	count := 0
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		count++
		return true
	})

	if count != 4 {
		t.Errorf("expected 4 registered functions, got %d", count)
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
}
