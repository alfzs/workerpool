package workerpool

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type TaskPriority int

const (
	PriorityLow TaskPriority = iota
	PriorityNormal
	PriorityHigh
)

type Task interface {
	Name() string
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error
	Priority() TaskPriority
	Timeout() *time.Duration
}

type PoolTask struct {
	Ctx        context.Context
	TenantID   uuid.UUID
	TaskName   string
	Task       Task
	CreatedAt  time.Time
	OnComplete func()
}
