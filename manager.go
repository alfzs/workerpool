// manager.go
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
	GetWorkerLimit() int
}

type tenantProvider interface {
	GetActive(ctx context.Context) ([]Tenant, error)
}

type taskExecutor interface {
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error
}

type RetryableError interface {
	error
	Retryable() bool
}

func IsRetryable(err error) bool {
	if re, ok := err.(RetryableError); ok {
		return re.Retryable()
	}
	return true
}

var ErrPermanent = fmt.Errorf("permanent error")

type WorkerManager struct {
	logger   *slog.Logger
	provider tenantProvider
	executor taskExecutor
	config   Config
	pool     *pool

	ctx    context.Context
	cancel context.CancelFunc

	tenants sync.Map

	wg       sync.WaitGroup
	stopping atomic.Bool
}

type tenantState struct {
	id uuid.UUID

	ctx    context.Context
	cancel context.CancelFunc

	queue chan struct{}

	desiredWorkers atomic.Int32
	activeWorkers  atomic.Int32
	stopped        atomic.Bool

	wg sync.WaitGroup

	startWorkerFn func(*tenantState, int)
}

type WorkerManagerParams struct {
	Logger         *slog.Logger
	TenantProvider tenantProvider
	TaskExecutor   taskExecutor
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
		executor: p.TaskExecutor,
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

	w.tenants.Range(func(_, value any) bool {
		state := value.(*tenantState)
		state.stop()
		return true
	})

	w.wg.Wait()
	w.pool.stop()

	w.logger.Info("worker manager stopped")
}

func (w *WorkerManager) TriggerWithContext(ctx context.Context, tenantID uuid.UUID) error {
	value, ok := w.tenants.Load(tenantID)
	if !ok {
		return fmt.Errorf("tenant not found")
	}

	state := value.(*tenantState)

	if state.stopped.Load() {
		return fmt.Errorf("tenant stopped")
	}

	select {
	case state.queue <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *WorkerManager) Trigger(tenantID uuid.UUID) {
	_ = w.TriggerWithContext(context.Background(), tenantID)
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

	activeSet := make(map[uuid.UUID]Tenant, len(active))
	for _, tenant := range active {
		id := tenant.GetID()
		if id == uuid.Nil {
			continue
		}
		activeSet[id] = tenant
	}

	for id, tenant := range activeSet {
		limit := tenant.GetWorkerLimit()
		if limit <= 0 {
			limit = 1
		}

		value, exists := w.tenants.Load(id)
		if !exists {
			state := w.createTenant(id, limit)
			actual, loaded := w.tenants.LoadOrStore(id, state)
			if loaded {
				state.stop()
				existing := actual.(*tenantState)
				existing.scale(limit)
			}
			continue
		}

		state := value.(*tenantState)
		state.scale(limit)
	}

	w.tenants.Range(func(key, value any) bool {
		id := key.(uuid.UUID)
		if _, exists := activeSet[id]; exists {
			return true
		}
		state := value.(*tenantState)
		if w.tenants.CompareAndDelete(id, state) {
			state.stop()
		}
		return true
	})

	return nil
}

func (w *WorkerManager) createTenant(id uuid.UUID, workers int) *tenantState {
	ctx, cancel := context.WithCancel(w.ctx)

	state := &tenantState{
		id:            id,
		ctx:           ctx,
		cancel:        cancel,
		queue:         make(chan struct{}, workers*8),
		startWorkerFn: w.startWorker,
	}

	state.desiredWorkers.Store(int32(workers))

	for i := 0; i < workers; i++ {
		w.startWorker(state, i)
	}

	return state
}

type ctxKeyAttempt struct{}

func (w *WorkerManager) startWorker(state *tenantState, workerID int) {
	state.wg.Add(1)
	state.activeWorkers.Add(1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				w.logger.Error("panic in tenant worker",
					slog.String("tenant_id", state.id.String()),
					slog.Int("worker_id", workerID),
					slog.Any("panic", r))

				state.activeWorkers.Add(-1)
				state.wg.Done()

				if state.stopped.Load() {
					return
				}

				time.Sleep(time.Second)
				w.startWorker(state, workerID)
				return
			}
			state.activeWorkers.Add(-1)
			state.wg.Done()
		}()

		w.workerLoop(state, workerID)
	}()
}

func (w *WorkerManager) workerLoop(state *tenantState, workerID int) {
	for {
		desired := int(state.desiredWorkers.Load())
		active := int(state.activeWorkers.Load())

		if active > desired {
			return
		}

		select {
		case <-state.ctx.Done():
			return
		case <-state.queue:
			taskCtx, cancel := context.WithTimeout(state.ctx, w.config.TaskTimeout)

			task := Task{
				Ctx:      taskCtx,
				TaskID:   uuid.New(),
				TenantID: state.id,
				Executor: w.executor,
				Complete: cancel,
			}

			if err := w.pool.addTask(task); err != nil {
				cancel()
				w.logger.Warn("failed to submit task to pool",
					slog.String("tenant_id", state.id.String()),
					slog.Int("worker_id", workerID),
					slog.Any("error", err))
			}
		}
	}
}

func (t *tenantState) scale(newLimit int) {
	if newLimit <= 0 {
		newLimit = 1
	}

	old := int(t.desiredWorkers.Swap(int32(newLimit)))

	if newLimit <= old {
		return
	}

	diff := newLimit - old
	for i := 0; i < diff; i++ {
		t.startWorkerFn(t, old+i)
	}
}

func (t *tenantState) stopWithTimeout(timeout time.Duration) {
	if !t.stopped.CompareAndSwap(false, true) {
		return
	}

	t.cancel()

	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		slog.Error("timeout stopping tenant", slog.String("tenant_id", t.id.String()))
	}
}

func (t *tenantState) stop() {
	t.stopWithTimeout(30 * time.Second)
}
