package workerpool

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/google/uuid"
)

type TaskPriority int

const (
	PriorityLow TaskPriority = iota
	PriorityNormal
	PriorityHigh
)

type Task interface {
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error
}

type RetryPredicate func(error) bool

type PanicError struct {
	TaskName string
	TenantID uuid.UUID
	WorkerID int
	Value    any
	Stack    string
}

func (e *PanicError) Error() string {
	return fmt.Sprintf(
		"panic in task=%s tenant=%s worker=%d value=%v",
		e.TaskName,
		e.TenantID,
		e.WorkerID,
		e.Value,
	)
}

var _ error = (*PanicError)(nil)

func capturePanic(taskName string, tenantID uuid.UUID, workerID int, r any) *PanicError {
	return &PanicError{
		TaskName: taskName,
		TenantID: tenantID,
		WorkerID: workerID,
		Value:    r,
		Stack:    string(debug.Stack()),
	}
}
