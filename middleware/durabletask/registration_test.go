package durabletask

import (
	"context"
	"strings"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/task"
)

func registrationTestOrchestrator(*task.OrchestrationContext) (any, error) { return nil, nil }

func registrationTestActivity(_ context.Context, name string) (string, error) { return name, nil }

// appHasFunction reports whether the app indexed a function under name.
func appHasFunction(app *sdk.App, name string) bool {
	found := false
	app.GetRegisteredFunctions().Range(func(_, value any) bool {
		if rf, ok := value.(*sdk.RegisteredFunction); ok && rf.FuncName == name {
			found = true
			return false
		}
		return true
	})
	return found
}

// Registering before Use is the supported order: App.Use collects the
// middleware's functions when it is called, so everything registered up to that
// point reaches the app.
func TestRegisterBeforeUse_ReachesTheApp(t *testing.T) {
	app := sdk.FunctionApp()
	d := Middleware()
	d.Orchestrator("BeforeUseOrchestration", registrationTestOrchestrator)
	d.Activity("BeforeUseActivity", registrationTestActivity)
	app.Use(d)

	for _, name := range []string{"BeforeUseOrchestration", "BeforeUseActivity"} {
		if !appHasFunction(app, name) {
			t.Errorf("expected %q to be registered with the app", name)
		}
	}
}

// The options form supplies everything at construction, so it cannot be
// registered too late by design.
func TestRegisterViaOptions_ReachesTheApp(t *testing.T) {
	app := sdk.FunctionApp()
	app.Use(Middleware(
		WithOrchestrator("OptionOrchestration", registrationTestOrchestrator),
		WithActivity("OptionActivity", registrationTestActivity),
	))

	for _, name := range []string{"OptionOrchestration", "OptionActivity"} {
		if !appHasFunction(app, name) {
			t.Errorf("expected %q to be registered with the app", name)
		}
	}
}

// Registering after Use cannot reach the app, so it must fail loudly rather
// than leave an orchestration that never runs.
func TestRegisterAfterUse_Panics(t *testing.T) {
	cases := []struct {
		name     string
		register func(*Durable)
	}{
		{"Orchestrator", func(d *Durable) { d.Orchestrator("TooLate", registrationTestOrchestrator) }},
		{"Activity", func(d *Durable) { d.Activity("TooLate", registrationTestActivity) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := sdk.FunctionApp()
			d := Middleware()
			app.Use(d)

			defer func() {
				recovered := recover()
				if recovered == nil {
					t.Fatalf("expected %s after Use to panic", tc.name)
				}
				message, ok := recovered.(string)
				if !ok {
					t.Fatalf("expected a string panic value, got %T", recovered)
				}
				// The message has to say what to do about it, not just that it broke.
				if !strings.Contains(message, "after app.Use") {
					t.Errorf("panic message should explain the ordering, got: %s", message)
				}
				if !strings.Contains(message, "TooLate") {
					t.Errorf("panic message should name the function, got: %s", message)
				}
			}()

			tc.register(d)
		})
	}
}

// A middleware that is never handed to an app stays open, which is the shape
// the durable tests use when they drive the middleware directly.
func TestRegisterWithoutUse_StaysOpen(t *testing.T) {
	d := Middleware()
	d.Orchestrator("First", registrationTestOrchestrator)
	d.Activity("Second", registrationTestActivity)

	if got := len(d.ProvidedFunctions()); got != 2 {
		t.Fatalf("expected 2 provided functions, got %d", got)
	}
}

// ProvidedFunctions is part of a public interface, so anything may call it to
// inspect what a middleware contributes. Reading it must not close
// registration: only App.Use does that, via SealRegistration. Before this was
// split, an introspecting caller made the next registration panic with a
// message blaming an app.Use call that never happened.
func TestProvidedFunctions_DoesNotCloseRegistration(t *testing.T) {
	d := Middleware()
	d.Orchestrator("First", registrationTestOrchestrator)

	// Inspect twice, the way a diagnostic or a wrapping middleware might.
	_ = d.ProvidedFunctions()
	_ = d.ProvidedFunctions()

	d.Activity("Second", registrationTestActivity)

	if got := len(d.ProvidedFunctions()); got != 2 {
		t.Fatalf("expected the later registration to be kept, got %d provided functions", got)
	}
}

// SealRegistration is what actually closes registration, and App.Use may call
// it more than once if the same middleware is registered twice.
func TestSealRegistration_IsIdempotentAndClosesRegistration(t *testing.T) {
	d := Middleware()
	d.Orchestrator("First", registrationTestOrchestrator)

	d.SealRegistration()
	d.SealRegistration()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected a registration after sealing to panic")
		}
		message, ok := recovered.(string)
		if !ok {
			t.Fatalf("expected a string panic value, got %T", recovered)
		}
		if !strings.Contains(message, "after app.Use") {
			t.Errorf("panic message should explain the ordering, got: %s", message)
		}
	}()

	d.Activity("TooLate", registrationTestActivity)
}
