package workerpool

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Task интерфейс, который должны реализовывать все задачи
type Task interface {
	// Execute выполняет задачу для конкретного тенанта
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error

	// GetName возвращает человекочитаемое имя задачи
	GetName() string

	// GetID возвращает уникальный идентификатор типа задачи
	// Важно: этот ID должен быть одинаковым для всех экземпляров одного типа задачи
	GetID() uuid.UUID

	// GetPriority возвращает приоритет задачи
	GetPriority() TaskPriority

	// GetTimeout возвращает опциональный таймаут для задачи
	// Если nil, используется глобальный таймаут из конфига
	GetTimeout() *time.Duration
}

// TaskPriority определяет приоритет задачи
type TaskPriority int

const (
	PriorityLow    TaskPriority = 0
	PriorityNormal TaskPriority = 1
	PriorityHigh   TaskPriority = 2
)
