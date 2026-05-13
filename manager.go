package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Tenant interface {
	GetID() uuid.UUID
}

type tenantProvider interface {
	GetActive(ctx context.Context) ([]Tenant, error)
}

type taskExecutor interface {
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error
}

type TenantTaskStats struct {
	TenantID    uuid.UUID
	ActiveTasks int
	QueuedTasks int
}

type WorkerManager struct {
	logger        *slog.Logger
	provider      tenantProvider
	config        Config
	pool          *pool
	ctx           context.Context
	cancel        context.CancelFunc
	activeTenants sync.Map
	wg            sync.WaitGroup
	stopping      atomic.Bool
}

type WorkerManagerParams struct {
	Logger         *slog.Logger
	TenantProvider tenantProvider
	Config         Config
}

func NewWorkerManager(p WorkerManagerParams) (*WorkerManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	pool, err := newPool(PoolParams{
		Ctx:    ctx,
		Logger: p.Logger,
		Config: p.Config,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	return &WorkerManager{
		logger:   p.Logger.With(slog.String("component", "worker_manager")),
		provider: p.TenantProvider,
		config:   p.Config,
		pool:     pool,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (w *WorkerManager) Start() error {
	w.pool.start()
	if err := w.refreshTenants(); err != nil {
		w.pool.stop()
		return fmt.Errorf("initial tenant refresh failed: %w", err)
	}
	w.wg.Add(1)
	go w.refreshLoop()
	return nil
}

func (w *WorkerManager) Stop() {
	if !w.stopping.CompareAndSwap(false, true) {
		return
	}
	w.logger.Info("stopping worker manager")
	w.cancel()
	w.wg.Wait()
	w.pool.stop()
	w.logger.Info("worker manager stopped")
}

func (w *WorkerManager) TaskTrigger(ctx context.Context, tenantID uuid.UUID, executor taskExecutor, priority TaskPriority) error {
	if _, ok := w.activeTenants.Load(tenantID); !ok {
		return fmt.Errorf("tenant %s is not active", tenantID)
	}

	taskCtx, cancel := context.WithTimeout(ctx, w.config.TaskTimeout)
	task := &Task{
		Ctx:      taskCtx,
		TaskID:   uuid.New(),
		TenantID: tenantID,
		Executor: &tenantTaskExecutor{
			executor:    executor,
			taskContext: taskCtx,
		},
		Priority: priority,
		Complete: cancel,
	}

	return w.pool.addTask(task)
}

func (w *WorkerManager) refreshLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.refreshTenants(); err != nil {
				w.logger.Error("failed to refresh tenants", slog.Any("error", err))
			}
		}
	}
}

func (w *WorkerManager) refreshTenants() error {
	active, err := w.provider.GetActive(w.ctx)
	if err != nil {
		return fmt.Errorf("get active tenants: %w", err)
	}

	newActive := make(map[uuid.UUID]struct{})
	for _, tenant := range active {
		id := tenant.GetID()
		if id == uuid.Nil {
			continue
		}
		newActive[id] = struct{}{}
		w.activeTenants.Store(id, struct{}{})
	}

	w.activeTenants.Range(func(key, value interface{}) bool {
		id := key.(uuid.UUID)
		if _, ok := newActive[id]; !ok {
			w.activeTenants.Delete(id)
		}
		return true
	})

	SetActiveTenants(int64(len(newActive)))
	return nil
}

func (w *WorkerManager) GetTenantStats(tenantID uuid.UUID) (*TenantTaskStats, error) {
	if _, ok := w.activeTenants.Load(tenantID); !ok {
		return nil, fmt.Errorf("tenant not found or inactive")
	}
	return &TenantTaskStats{
		TenantID:    tenantID,
		ActiveTasks: 0,
		QueuedTasks: 0,
	}, nil
}
