package worker

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// runUserInvocation composes the middleware chain around inner, executes
// it, and converts both errors AND panics into normal return values.
//
// Both the gRPC-body and HTTP-streaming paths run user code through
// app.Compose; they previously diverged on panic handling (HTTP recovered
// inline, gRPC let the panic propagate to the dispatch goroutine). This
// helper centralizes the contract:
//
//   - If the user code panics, recovered carries the recovered value
//     and stack contains the formatted stack trace captured at recover
//     time. err is nil.
//   - If the user code returns an error, it is returned as err.
//   - On success, all three return values are zero.
//
// app may be nil (defensive: a worker without registered middleware
// passes a nil App). In that case the inner handler is invoked directly.
func runUserInvocation(ctx context.Context, mc *sdk.MiddlewareContext, app *sdk.App,
	inner sdk.Handler) (recovered any, stack string, err error) {

	defer func() {
		if r := recover(); r != nil {
			recovered = r
			stack = string(debug.Stack())
		}
	}()

	if app == nil {
		return nil, "", inner(ctx, mc)
	}
	chain := app.Compose(inner)
	return nil, "", chain(ctx, mc)
}

// statusFromInvocation converts the (recovered, stack, err) triple
// returned by [runUserInvocation] into the StatusResult that goes on the
// InvocationResponse. Panics take precedence over errors because a panic
// indicates the user code never returned normally, so any returned error
// would be uninitialized.
//
// The Source field is set to "User function" for both panic and error
// cases -- the host uses it to attribute the failure in the Functions
// portal / App Insights.
func statusFromInvocation(recovered any, stack string, err error) *pb.StatusResult {
	switch {
	case recovered != nil:
		return &pb.StatusResult{
			Status: pb.StatusResult_Failure,
			Exception: &pb.RpcException{
				Message:         fmt.Sprintf("%v", recovered),
				Source:          "User function",
				StackTrace:      stack,
				IsUserException: true,
			},
		}
	case err != nil:
		return &pb.StatusResult{
			Status: pb.StatusResult_Failure,
			Exception: &pb.RpcException{
				Message:         err.Error(),
				Source:          "User function",
				IsUserException: true,
			},
		}
	default:
		return &pb.StatusResult{Status: pb.StatusResult_Success}
	}
}
