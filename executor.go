package workerpool

import (
	"context"

	"github.com/google/uuid"
)

// tenantTaskExecutor is a wrapper that binds a task executor to a specific context.
// It implements the taskExecutor interface and delegates execution to the underlying
// executor while using the pre-bound context instead of the one passed in.
//
// This is useful when you need to capture a specific context at task creation time
// (e.g., with trace IDs, timeouts, or values) and ensure that same context is used
// for all execution attempts, regardless of when they happen.
//
// Example:
//
//	ctxWithTrace := tracing.EnsureTraceID(parentCtx)
//	executor := &tenantTaskExecutor{
//	    executor:    myExecutor,
//	    taskContext: ctxWithTrace,
//	}
//
//	// All retries will use ctxWithTrace, preserving trace ID
//	executor.Execute(someOtherCtx, tenantID, workerID)
type tenantTaskExecutor struct {
	// executor is the underlying task executor that performs the actual work
	executor taskExecutor

	// taskContext is the context that will be used for all executions.
	// It overrides the context parameter in Execute().
	taskContext context.Context
}

// Execute implements the taskExecutor interface.
// It ignores the provided ctx parameter and uses the pre-bound taskContext instead.
// This ensures consistent context across retry attempts.
func (e *tenantTaskExecutor) Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error {
	// Note: The ctx parameter is deliberately ignored in favor of e.taskContext.
	// This allows the context (with trace IDs, timeouts, etc.) to be captured
	// at task creation time and reused across all retry attempts.
	return e.executor.Execute(e.taskContext, tenantID, workerID)
}
