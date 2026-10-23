package durabletask

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
)

// EntityOperationHandler handles one operation from a Functions entity batch.
// The Functions entity contract does not expose whether an operation was sent
// as a call or a signal, so this context deliberately has no IsSignal field.
type EntityOperationHandler func(*EntityContext) (any, error)

// EntityFactory creates an entity handler for one batch.
type EntityFactory func(EntityFactoryContext) (EntityBatch, error)

// EntityFactoryContext identifies the entity batch being created.
type EntityFactoryContext struct {
	Context context.Context
	ID      api.EntityID
}

// EntityBatch contains the handler and optional cleanup for one entity batch.
type EntityBatch struct {
	Handler EntityOperationHandler
	Close   func(context.Context) error
}

// EntityContext is the Functions-compatible context for one entity operation.
// It delegates state and action semantics to durabletask-go while exposing
// only metadata that the Functions host delivers.
type EntityContext struct {
	ID            api.EntityID
	OperationName string
	RequestID     string
	BatchPosition int
	BatchSize     int

	invocationContext context.Context
	upstream          *task.EntityContext
}

// Context returns the invocation context, including cancellation and tracing.
// It does not attach upstream's IsSignal metadata because the Functions
// entity-trigger envelope does not carry that value.
func (ctx *EntityContext) Context() context.Context {
	return ctx.invocationContext
}

// HasInput reports whether this operation supplied input.
func (ctx *EntityContext) HasInput() bool {
	return ctx.upstream.HasInput()
}

// GetInput decodes the operation input into value.
func (ctx *EntityContext) GetInput(value any) error {
	return ctx.upstream.GetInput(value)
}

// HasState reports whether the entity currently has persistent state.
func (ctx *EntityContext) HasState() bool {
	return ctx.upstream.HasState()
}

// GetState decodes the current persistent state into value.
func (ctx *EntityContext) GetState(value any) error {
	return ctx.upstream.GetState(value)
}

// SetState replaces the entity's persistent state.
func (ctx *EntityContext) SetState(value any) error {
	return ctx.upstream.SetState(value)
}

// DeleteState removes the entity's persistent state.
func (ctx *EntityContext) DeleteState() {
	ctx.upstream.DeleteState()
}

// GetRawState returns the serialized persistent state and whether it exists.
func (ctx *EntityContext) GetRawState() (string, bool) {
	return ctx.upstream.GetRawState()
}

// SetRawState replaces the serialized persistent state. An empty string
// deletes state, matching durabletask-go's entity contract.
func (ctx *EntityContext) SetRawState(state string) {
	ctx.upstream.SetRawState(state)
}

// SignalEntity sends a fire-and-forget operation to another entity.
func (ctx *EntityContext) SignalEntity(id api.EntityID, operation string, input any) error {
	return ctx.upstream.SignalEntity(id, operation, input)
}

// SignalEntityAt schedules a fire-and-forget operation to another entity.
func (ctx *EntityContext) SignalEntityAt(
	id api.EntityID,
	scheduledTime time.Time,
	operation string,
	input any,
) error {
	return ctx.upstream.SignalEntityAt(id, scheduledTime, operation, input)
}

// StartNewOrchestration starts an orchestration from this entity operation.
func (ctx *EntityContext) StartNewOrchestration(name string, options ...task.EntityStartOrchestrationOption) error {
	return ctx.upstream.StartNewOrchestration(name, options...)
}

type entityBatchInfoKey struct{}

type entityBatchInfo struct {
	size     int
	position atomic.Int32
}

func entityBatchInfoFromContext(ctx context.Context) *entityBatchInfo {
	if info, ok := ctx.Value(entityBatchInfoKey{}).(*entityBatchInfo); ok && info != nil {
		return info
	}
	return &entityBatchInfo{}
}

func wrapEntityFactory(factory EntityFactory) task.EntityFactory {
	return func(upstreamContext task.EntityFactoryContext) (task.EntityBatch, error) {
		batch, err := factory(EntityFactoryContext{
			Context: upstreamContext.Context,
			ID:      upstreamContext.ID,
		})
		if err != nil {
			return task.EntityBatch{}, err
		}
		if batch.Handler == nil {
			return task.EntityBatch{}, fmt.Errorf("entity factory returned a nil handler")
		}

		info := entityBatchInfoFromContext(upstreamContext.Context)
		return task.EntityBatch{
			Entity: func(upstreamOperation *task.EntityContext) (any, error) {
				position := int(info.position.Add(1) - 1)
				return batch.Handler(&EntityContext{
					ID:                upstreamOperation.ID,
					OperationName:     upstreamOperation.Operation,
					RequestID:         upstreamOperation.RequestID,
					BatchPosition:     position,
					BatchSize:         info.size,
					invocationContext: upstreamContext.Context,
					upstream:          upstreamOperation,
				})
			},
			Close: batch.Close,
		}, nil
	}
}
