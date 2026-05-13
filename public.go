package workerpool

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

type WorkerManager struct {
	pool *pool
}

func NewWorkerManager(logger *slog.Logger, cfg Config, retry RetryPredicate) *WorkerManager {
	return &WorkerManager{
		pool: newPool(logger, cfg, retry),
	}
}

func (m *WorkerManager) Start() { m.pool.Start() }
func (m *WorkerManager) Stop()  { m.pool.Stop() }

func (m *WorkerManager) Execute(ctx context.Context, taskName string, tenantID uuid.UUID) error {
	task := &PoolTask{
		TaskName: taskName,
		TenantID: tenantID,
		Ctx:      ctx,
	}

	if err := m.pool.submit(task); err != nil {
		return fmt.Errorf("submit: %w", err)
	}

	return nil
}
