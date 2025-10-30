package workerpool

import (
	"context"

	"github.com/google/uuid"
)

type tenantTaskExecutor struct {
	executor    taskExecutor
	taskContext context.Context
}

func (e *tenantTaskExecutor) Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error {
	return e.executor.Execute(e.taskContext, tenantID, workerID)
}
