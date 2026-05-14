package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

type taskLocks struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newTaskLocks() *taskLocks {
	return &taskLocks{
		m: make(map[string]struct{}),
	}
}

func (l *taskLocks) TryLock(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.m[key]; exists {
		return false
	}

	l.m[key] = struct{}{}

	return true
}

func (l *taskLocks) Unlock(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.m, key)
}

type WorkerManager struct {
	logger *slog.Logger

	pool *pool

	config Config

	registry *Registry

	running *taskLocks
}

type WorkerManagerParams struct {
	Logger   *slog.Logger
	Config   Config
	Registry *Registry
}

func NewWorkerManager(p WorkerManagerParams) *WorkerManager {
	return &WorkerManager{
		logger: p.Logger,
		pool: newPool(
			p.Logger,
			p.Config,
			nil,
		),
		config:   p.Config,
		registry: p.Registry,
		running:  newTaskLocks(),
	}
}

func (m *WorkerManager) Start() {
	m.pool.start()
}

func (m *WorkerManager) Stop() {
	m.pool.stop()
}

func (m *WorkerManager) ExecuteTask(ctx context.Context, taskName string, tenantID uuid.UUID) error {
	key := fmt.Sprintf(
		"%s:%s",
		tenantID.String(),
		taskName,
	)

	if !m.running.TryLock(key) {
		return fmt.Errorf(
			"task already running",
		)
	}

	task, err := m.registry.Create(taskName)
	if err != nil {
		m.running.Unlock(key)
		return err
	}

	timeout := m.config.TaskTimeout

	if t := task.Timeout(); t != nil {
		timeout = *t
	}

	taskCtx, cancel := context.WithTimeout(m.pool.context(), timeout)

	poolTask := &PoolTask{
		Ctx:       taskCtx,
		TenantID:  tenantID,
		TaskName:  task.Name(),
		Task:      task,
		CreatedAt: time.Now(),
		OnComplete: func() {
			cancel()
			m.running.Unlock(key)
		},
	}

	if err := m.pool.submit(poolTask); err != nil {
		cancel()
		m.running.Unlock(key)
		return err
	}

	return nil
}
