package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

type LoadedFunction struct {
	Function sdk.RegisteredFunction
	Fields   map[string]*funcField
}

func handleWorkerInitRequest(req *pb.WorkerInitRequest, requestId string, disp *Dispatcher) *pb.StreamingMessage {
	logger := disp.SystemLogger()
	logger.LogAttrs(context.Background(), slog.LevelInfo, "Received WorkerInitRequest",
		slog.String("request_id", requestId),
	)

	// Capabilities the Go worker advertises. These mirror what the Functions
	// host expects from a modern out-of-proc worker (Python / dotnet-isolated
	// declare a similar set). When the worker also has HTTP triggers and
	// successfully started the loopback HTTP proxy, "HttpUri" is added — the
	// host will then forward HTTP requests to that URL via YARP and skip the
	// gRPC body for HTTP triggers, enabling true streaming.
	capabilities := map[string]string{
		"TypedDataCollection":               "true",
		"WorkerStatus":                      "true",
		"RpcHttpBodyOnly":                   "true",
		"RawHttpBodyBytes":                  "true",
		"RpcHttpTriggerMetadataRemoved":     "true",
		"UseNullableValueDictionaryForHttp": "true",
		"HandlesWorkerTerminateMessage":     "true",
	}

	// Merge in capabilities advertised by the user app (e.g. middleware that
	// implements sdk.CapabilityProvider). Middleware-supplied keys win over
	// the static base on collision so users can opt out of a default if they
	// have a reason to.
	if disp != nil && disp.App != nil {
		for k, v := range disp.App.Capabilities() {
			capabilities[k] = v
		}
	}

	if disp != nil && disp.HTTPProxy != nil {
		capabilities["HttpUri"] = disp.HTTPProxy.url
		// Required so route parameters still flow via gRPC trigger metadata
		// when the body is being proxied over HTTP.
		capabilities["RequiresRouteParameters"] = "true"
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Advertising HttpUri for streaming HTTP triggers",
			slog.String("http_uri", disp.HTTPProxy.url),
		)
	}

	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_WorkerInitResponse{
			WorkerInitResponse: &pb.WorkerInitResponse{
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
					Result: "Success",
				},
				WorkerVersion:  "1.0.0",
				Capabilities:   capabilities,
				WorkerMetadata: buildWorkerMetadata(),
			},
		},
	}
}

func handleFunctionsMetadataRequest(req *pb.FunctionsMetadataRequest, app *sdk.App, requestId string) *pb.StreamingMessage {
	var functions []*pb.RpcFunctionMetadata
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*sdk.RegisteredFunction)

		// Map bindings.Binding to pb.BindingInfo
		bindingsMap := make(map[string]*pb.BindingInfo)
		var rawBindings []string

		for _, b := range rf.RawBindings {
			var dir pb.BindingInfo_Direction
			switch b.Direction {
			case "in":
				dir = pb.BindingInfo_in
			case "out":
				dir = pb.BindingInfo_out
			case "inout":
				dir = pb.BindingInfo_inout
			}

			bindingsMap[b.Name] = &pb.BindingInfo{
				Type:      b.Type,
				Direction: dir,
				DataType:  pb.BindingInfo_undefined,
			}

			if bData, err := json.Marshal(b); err == nil {
				rawBindings = append(rawBindings, string(bData))
			}
		}

		functions = append(functions, &pb.RpcFunctionMetadata{
			FunctionId:  rf.FuncId,
			Name:        rf.FuncName,
			Bindings:    bindingsMap,
			RawBindings: rawBindings,
			Language:    "go",
			ScriptFile:  rf.ScriptFile,
			EntryPoint:  rf.FuncName,
		})
		return true
	})

	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_FunctionMetadataResponse{
			FunctionMetadataResponse: &pb.FunctionMetadataResponse{
				FunctionMetadataResults: functions,
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
					Result: "Success",
				},
			},
		},
	}
}

func handleFunctionLoadRequest(req *pb.FunctionLoadRequest, disp *Dispatcher, requestId string) *pb.StreamingMessage {
	funcID := req.FunctionId
	val, ok := disp.App.GetRegisteredFunctions().Load(funcID)
	if !ok {
		return &pb.StreamingMessage{
			RequestId: requestId,
			Content: &pb.StreamingMessage_FunctionLoadResponse{
				FunctionLoadResponse: &pb.FunctionLoadResponse{
					FunctionId: funcID,
					Result: &pb.StatusResult{
						Status: pb.StatusResult_Failure,
						Exception: &pb.RpcException{
							Message: fmt.Sprintf("Function with ID %s not found", funcID),
						},
					},
				},
			},
		}
	}

	rf := val.(*sdk.RegisteredFunction)
	ft := reflect.TypeOf(rf.Func)
	fields := make(map[string]*funcField)

	// Identify writer index for HTTP triggers
	writerIndex := -1
	for i := 0; i < ft.NumIn(); i++ {
		if ft.In(i) == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
			writerIndex = i
			break
		}
	}

	// If writer exists, map $return binding to it
	if writerIndex != -1 {
		fields["$return"] = &funcField{
			Name:       "$return",
			Type:       ft.In(writerIndex),
			Position:   writerIndex,
			Direction:  "out",
			IsArgument: true,
			IsWriter:   true,
		}
	}

	// Map trigger input binding to the appropriate argument
	argIndex := 0
	for _, b := range rf.RawBindings {
		if b.Direction != "in" {
			continue
		}

		// Find the next argument that is not context.Context or http.ResponseWriter
		for argIndex < ft.NumIn() {
			argType := ft.In(argIndex)
			if argType.Implements(contextType) || argType == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
				argIndex++
				continue
			}
			break
		}

		if argIndex >= ft.NumIn() {
			break
		}

		fields[b.Name] = &funcField{
			Name:       b.Name,
			Type:       ft.In(argIndex),
			Position:   argIndex,
			Direction:  b.Direction,
			IsArgument: true,
		}
		argIndex++
	}

	logger := disp.SystemLogger()
	for k, v := range fields {
		logger.LogAttrs(context.Background(), slog.LevelDebug, "FunctionLoad field mapping",
			slog.String("function_id", funcID),
			slog.String("name", k),
			slog.Int("position", v.Position),
			slog.String("type", v.Type.String()),
			slog.String("direction", v.Direction),
			slog.Bool("is_argument", v.IsArgument),
		)
	}

	disp.LoadedFunctions.Store(funcID, &LoadedFunction{
		Function: *rf,
		Fields:   fields,
	})

	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_FunctionLoadResponse{
			FunctionLoadResponse: &pb.FunctionLoadResponse{
				FunctionId: funcID,
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
					Result: "Success",
				},
			},
		},
	}
}

func handleInvocationRequest(req *pb.InvocationRequest, disp *Dispatcher, requestId string) (*pb.StreamingMessage, error) {
	funcID := req.FunctionId
	val, ok := disp.LoadedFunctions.Load(funcID)
	if !ok {
		return nil, fmt.Errorf("function with ID %s not loaded", funcID)
	}
	loadedFunc := val.(*LoadedFunction)

	// HTTP streaming path: when the host is forwarding HTTP via the
	// "HttpUri" capability, the user handler runs against the live
	// http.ResponseWriter inside the embedded HTTP proxy. The gRPC side
	// just waits for completion and returns a minimal InvocationResponse.
	// Pass disp.App so the proxy can compose the registered middleware
	// chain (notably otelfunc) around the user handler -- without this,
	// HTTP-streaming invocations would bypass middleware entirely.
	if disp.HTTPProxy != nil && isHTTPHandler(loadedFunc) && isHTTPProxiedInvocation(req) {
		status, err := disp.HTTPProxy.notifyGRPCArrival(req, loadedFunc, disp.App)
		if err != nil {
			// Don't return a Go error — handleBidiStream treats those as
			// fatal. A rendezvous failure (timeout, cancelled HTTP request)
			// is a transient per-invocation problem, not a worker-level
			// crash. Surface it to the host as a Failure InvocationResponse
			// so this invocation reports an error and the worker stays up
			// to serve the next one.
			disp.SystemLogger().Error("HTTP proxy rendezvous failed",
				"invocation_id", req.GetInvocationId(), "err", err)
			status = &pb.StatusResult{
				Status: pb.StatusResult_Failure,
				Exception: &pb.RpcException{
					Message: err.Error(),
					Source:  "HTTP proxy rendezvous",
				},
			}
		}
		return &pb.StreamingMessage{
			RequestId: requestId,
			Content: &pb.StreamingMessage_InvocationResponse{
				InvocationResponse: &pb.InvocationResponse{
					InvocationId: req.InvocationId,
					Result:       status,
				},
			},
		}, nil
	}

	// Build the per-invocation context that user handlers and middleware will
	// see. Stash it on context.Context — every handler that takes a
	// context.Context can retrieve it via sdk.FromContext.
	ic := buildInvocationContext(req, &loadedFunc.Function)
	ctx := sdk.NewContext(context.Background(), ic)

	// Emit a system log with the inbound trace_parent so OTel correlation
	// can be debugged from production logs without instrumenting user code.
	disp.SystemLogger().Debug("InvocationRequest trace context",
		"invocation_id", ic.InvocationID,
		"trace_parent", ic.TraceContext.TraceParent,
		"trace_state", ic.TraceContext.TraceState,
		"baggage_count", len(ic.TraceContext.Baggage),
	)

	ft := reflect.TypeOf(loadedFunc.Function.Func)
	args := make([]reflect.Value, ft.NumIn())

	// 1. Pre-allocate arguments. context.Context arguments are seeded with
	//    the per-invocation ctx; middleware running between this point and
	//    the actual user-function call can swap in an enriched ctx via the
	//    inner handler below.
	for i := 0; i < ft.NumIn(); i++ {
		t := ft.In(i)
		if t.Implements(contextType) {
			args[i] = reflect.ValueOf(ctx)
			continue
		}

		if t == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
			args[i] = reflect.ValueOf(NewResponseWriterProxy())
			continue
		}

		if t.Kind() == reflect.Ptr {
			args[i] = reflect.New(t.Elem())
		} else {
			args[i] = reflect.Zero(t)
		}
	}

	// 2. Populate trigger input
	if err := FromProto(req, loadedFunc.Fields, args); err != nil {
		return nil, err
	}

	// 2b. If the function has a ClientFactory, use it to create the trigger client
	if loadedFunc.Function.ClientFactory != nil {
		// Extract trigger binding config
		config := make(map[string]any)
		if len(loadedFunc.Function.RawBindings) > 0 {
			bindingJSON, err := json.Marshal(loadedFunc.Function.RawBindings[0])
			if err == nil {
				json.Unmarshal(bindingJSON, &config)
			}
		}

		clientVal, err := loadedFunc.Function.ClientFactory(config, ic.TriggerMetadata)
		if err != nil {
			return nil, fmt.Errorf("client factory error: %v", err)
		}

		// Find the argument position for the client (skip context, writer, and already-populated args)
		for i := 0; i < ft.NumIn(); i++ {
			t := ft.In(i)
			if t.Implements(contextType) || t == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
				continue
			}
			// This is the trigger argument — replace it with the client
			args[i] = reflect.ValueOf(clientVal)
			break
		}
	}

	// 3. Build the innermost Handler — this is what every Middleware
	//    registered via App.Use ultimately wraps. It receives the (possibly
	//    enriched) ctx from the outer chain, propagates it into the user
	//    function's arguments, performs the reflective call, and returns
	//    any error result. See [sdk.App.Compose] for ordering.
	inner := func(ctx context.Context, _ *sdk.InvocationContext) error {
		injectInvocationContext(ft, args, ctx)
		fv := reflect.ValueOf(loadedFunc.Function.Func)
		results := fv.Call(args)
		for _, r := range results {
			if r.Type().Implements(errorType) && !r.IsNil() {
				return r.Interface().(error)
			}
		}
		return nil
	}

	// 4. Compose the middleware chain around the inner handler and run it.
	chain := disp.App.Compose(inner)
	invokeErr := chain(ctx, ic)

	// 5. Build response status from any error returned by the chain.
	status := &pb.StatusResult{
		Status: pb.StatusResult_Success,
	}
	if invokeErr != nil {
		status = &pb.StatusResult{
			Status: pb.StatusResult_Failure,
			Exception: &pb.RpcException{
				Message: invokeErr.Error(),
				Source:  "User function",
			},
		}
	}

	// 6. Extract HTTP response if applicable
	var returnValue *pb.TypedData
	for _, field := range loadedFunc.Fields {
		if field.Direction == "out" && field.IsWriter && field.Name == "$return" {
			if field.Position < len(args) {
				if proxy, ok := args[field.Position].Interface().(*ResponseWriterProxy); ok {
					returnValue = encodeHTTPResponse(proxy)
				}
			}
		}
	}

	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_InvocationResponse{
			InvocationResponse: &pb.InvocationResponse{
				InvocationId:           req.InvocationId,
				ReturnValue:            returnValue,
				Result:                 status,
				TraceContextAttributes: ic.OutboundTraceAttributes,
			},
		},
	}, nil
}

// errorType is the reflected type of the standard error interface, cached
// for use in the reflective invocation path.
var errorType = reflect.TypeOf((*error)(nil)).Elem()

// httpRequestPtrType is the reflected type of *http.Request, cached for
// use when injecting an enriched context.Context into HTTP handler args.
var httpRequestPtrType = reflect.TypeOf((*http.Request)(nil))

// buildInvocationContext converts a gRPC InvocationRequest plus the
// resolved RegisteredFunction into the public sdk.InvocationContext that
// user handlers and middleware see. Centralized so the HTTP-streaming
// path (which receives invocations via the embedded loopback HTTP server
// instead of the gRPC body) can construct the same value.
//
// HTTP-streaming integration: the streaming code in http_proxy.go must
// build an InvocationContext via this helper, stash it on r.Context() via
// sdk.NewContext, and run the user http.HandlerFunc inside an inner
// Handler wrapped by disp.App.Compose(inner).
// This ensures middleware (notably otelfunc.Middleware) wraps both the
// gRPC-body and HttpUri-proxied invocation paths uniformly.
func buildInvocationContext(req *pb.InvocationRequest, fn *sdk.RegisteredFunction) *sdk.InvocationContext {
	return &sdk.InvocationContext{
		InvocationID:    req.GetInvocationId(),
		FunctionID:      req.GetFunctionId(),
		FunctionName:    fn.FuncName,
		TriggerType:     fn.TriggerType,
		TraceContext:    convertTraceContext(req.GetTraceContext()),
		RetryContext:    convertRetryContext(req.GetRetryContext()),
		TriggerMetadata: flattenTriggerMetadata(req.GetTriggerMetadata()),
	}
}

// injectInvocationContext propagates ctx into all argument slots that need it
// before the user function is called. This runs after middleware has had a
// chance to enrich ctx (for example, by attaching an OpenTelemetry span via
// otelfunc.Middleware).
//
// Two slot kinds receive ctx:
//   - context.Context arguments (covers all non-HTTP triggers): the slot is
//     overwritten with the latest ctx.
//   - *http.Request arguments (HTTP triggers, which take http.HandlerFunc and
//     have no separate context.Context arg): the existing request is replaced
//     with req.WithContext(ctx) so that downstream code calling r.Context()
//     observes the enriched context.
func injectInvocationContext(ft reflect.Type, args []reflect.Value, ctx context.Context) {
	for i := 0; i < ft.NumIn(); i++ {
		t := ft.In(i)
		switch {
		case t.Implements(contextType):
			args[i] = reflect.ValueOf(ctx)
		case t == httpRequestPtrType:
			if r, ok := args[i].Interface().(*http.Request); ok && r != nil {
				args[i] = reflect.ValueOf(r.WithContext(ctx))
			}
		}
	}
}

// flattenTriggerMetadata reduces the host's TypedData metadata map to a
// simple string→string map for ergonomic consumption by user code and
// middleware. Non-string values are skipped (they are rare in practice;
// most trigger metadata fields are already strings).
func flattenTriggerMetadata(in map[string]*pb.TypedData) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if s := v.GetString_(); s != "" {
			out[k] = s
		}
	}
	return out
}

// convertTraceContext maps the host's RpcTraceContext to the SDK's
// public TraceContext type. A nil input produces a zero-valued result so
// callers (and middleware) can safely read the fields without nil checks.
func convertTraceContext(tc *pb.RpcTraceContext) sdk.TraceContext {
	if tc == nil {
		return sdk.TraceContext{}
	}
	return sdk.TraceContext{
		TraceParent: tc.GetTraceParent(),
		TraceState:  tc.GetTraceState(),
		Attributes:  tc.GetAttributes(),
		Baggage:     tc.GetBaggage(),
	}
}

// convertRetryContext maps the host's RetryContext to the SDK's public
// RetryContext type. A nil input produces a zero-valued result.
func convertRetryContext(rc *pb.RetryContext) sdk.RetryContext {
	if rc == nil {
		return sdk.RetryContext{}
	}
	return sdk.RetryContext{
		RetryCount:    rc.GetRetryCount(),
		MaxRetryCount: rc.GetMaxRetryCount(),
	}
}

func handleWorkerStatusRequest(requestId string, req *pb.WorkerStatusRequest) (*pb.StreamingMessage, error) {
	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_WorkerStatusResponse{
			WorkerStatusResponse: &pb.WorkerStatusResponse{},
		},
	}, nil
}

func handleWorkerTerminate(requestId string, req *pb.WorkerTerminate) (*pb.StreamingMessage, error) {
	return nil, nil
}

func handleFunctionEnvironmentReloadRequest(requestId string, req *pb.FunctionEnvironmentReloadRequest) (*pb.StreamingMessage, error) {
	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_FunctionEnvironmentReloadResponse{
			FunctionEnvironmentReloadResponse: &pb.FunctionEnvironmentReloadResponse{
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
					Result: "Success",
				},
				WorkerMetadata: buildWorkerMetadata(),
			},
		},
	}, nil
}
