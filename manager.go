package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type taskRegistry interface {
	Create(name string) (Task, error)
}

type Manager struct {
	pool     *pool
	registry taskRegistry
	log      *slog.Logger

	mu    sync.Mutex
	locks map[string]struct{}
}

func NewManager(log *slog.Logger, cfg Config, reg taskRegistry, retry RetryPredicate) *Manager {
	return &Manager{
		pool:     newPool(log, cfg, retry),
		registry: reg,
		log:      log,
		locks:    make(map[string]struct{}),
	}
}

func (m *Manager) Start() { m.pool.Start() }
func (m *Manager) Stop()  { m.pool.Stop() }

func (m *Manager) Execute(ctx context.Context, name string, tenant uuid.UUID) error {
	key := tenant.String() + ":" + name

	m.mu.Lock()
	if _, ok := m.locks[key]; ok {
		m.mu.Unlock()
		return fmt.Errorf("already running")
	}
	m.locks[key] = struct{}{}
	m.mu.Unlock()

	task, err := m.registry.Create(name)
	if err != nil {
		m.unlock(key)
		return err
	}

	taskCtx, cancel := context.WithCancel(m.pool.ctx)

	pt := &PoolTask{
		Ctx:      taskCtx,
		TaskName: name,
		TenantID: tenant,
		Executor: task,
		OnComplete: func() {
			cancel()
			m.unlock(key)
		},
	}

	if err := m.pool.submit(pt); err != nil {
		cancel()
		m.unlock(key)
		return err
	}

	return nil
}

func (m *Manager) ExecuteAll(ctx context.Context, name string, tenants []uuid.UUID) error {
	g, _ := errgroup.WithContext(ctx)

	for _, t := range tenants {
		t := t
		g.Go(func() error {
			return m.Execute(ctx, name, t)
		})
	}

	return g.Wait()
}

func (m *Manager) unlock(key string) {
	m.mu.Lock()
	delete(m.locks, key)
	m.mu.Unlock()
}
