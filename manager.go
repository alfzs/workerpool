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

// Tenant represents a customer or entity that requires isolated task execution.
// Implementations must provide thread-safe access to ID and worker limit.
type Tenant interface {
	// GetID returns the unique identifier for this tenant.
	GetID() uuid.UUID

	// GetWorkerLimit returns the maximum number of concurrent workers for this tenant.
	// This value can change dynamically and will be respected at runtime.
	GetWorkerLimit() int
}

// tenantProvider is an internal interface for fetching active tenants.
// The implementation should cache results to avoid excessive load on the backing store.
type tenantProvider interface {
	// GetActive returns the list of currently active tenants.
	// This method is called periodically and during initialization.
	GetActive(ctx context.Context) ([]Tenant, error)
}

// taskExecutor is an internal interface for executing tenant tasks.
// Implementations should be idempotent and handle context cancellation.
type taskExecutor interface {
	// Execute performs the actual work for a tenant task.
	// The provided context includes timeout and cancellation signals.
	// workerID identifies which worker is executing the task (0..limit-1).
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error
}

// WorkerManager orchestrates task execution across multiple tenants.
// It maintains per-tenant queues and enforces concurrency limits.
type WorkerManager struct {
	logger   *slog.Logger
	provider tenantProvider
	config   Config
	pool     *pool

	ctx    context.Context
	cancel context.CancelFunc

	tenantsMu sync.RWMutex
	tenants   map[uuid.UUID]*tenantState

	wg       sync.WaitGroup
	stopping atomic.Bool
}

// tenantState holds runtime state for a single tenant.
//
// taskQueue is never closed: it has multiple senders (any goroutine calling
// SubmitTask) and multiple readers across worker generations, so closing it
// would race with SubmitTask and could panic on send-to-closed-channel.
// Lifecycle is signalled purely via context cancellation.
//
// limit/genCancel are only mutated under WorkerManager.tenantsMu (by
// createTenant/refreshTenants), so no separate lock is needed here.
type tenantState struct {
	id uuid.UUID

	// taskQueue is a channel for incoming tasks. Sized once at creation based
	// on the initial limit; later limit changes do not resize it.
	taskQueue chan Task

	// limit is the number of workers in the current generation.
	limit int

	// genCancel cancels the current worker generation (derived from ctx).
	// Used to stop/restart workers when limit changes without touching
	// taskQueue or ctx.
	genCancel context.CancelFunc

	ctx    context.Context
	cancel context.CancelFunc
}

// WorkerManagerParams contains all dependencies required to create a WorkerManager.
type WorkerManagerParams struct {
	// Logger for structured logging. A component field will be added automatically.
	Logger *slog.Logger

	// TenantProvider supplies the list of active tenants.
	TenantProvider tenantProvider

	// Config contains all tunable parameters.
	Config Config
}

// NewWorkerManager creates a new WorkerManager with the provided dependencies.
// Returns an error if the underlying worker pool cannot be created.
func NewWorkerManager(p WorkerManagerParams) (*WorkerManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	pool, err := newPool(PoolParams{
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
		tenants:  make(map[uuid.UUID]*tenantState),
	}, nil
}

// Start initializes the worker manager and begins processing tasks.
// It starts the global worker pool, loads initial tenants, and begins tenant refresh cycle.
// Returns an error if the initial tenant load fails.
func (w *WorkerManager) Start() error {
	w.pool.start()

	if err := w.refreshTenants(); err != nil {
		w.pool.stop()
		return fmt.Errorf("initial tenant refresh failed: %w", err)
	}

	w.wg.Add(1)
	go w.tenantRefresher()

	return nil
}

// Stop gracefully shuts down the worker manager.
// It cancels all pending tasks, waits for running tasks to complete (up to GracefulTimeout),
// and releases all resources. This method blocks until shutdown is complete.
func (w *WorkerManager) Stop() {
	if w.stopping.Swap(true) {
		return
	}

	w.logger.Info("Stopping worker manager")
	w.cancel()

	// Cancelling each tenant's ctx (which is the parent of its current
	// genCtx) stops all workers. taskQueue is intentionally not closed
	// (see tenantState comment).
	//
	// Taking tenantsMu here, after stopping=true was set, establishes
	// happens-before with refreshTenants: refreshTenants checks stopping
	// under the same lock before doing wg.Add, so no wg.Add can race with
	// wg.Wait below.
	w.tenantsMu.Lock()
	for _, t := range w.tenants {
		t.cancel()
	}
	w.tenantsMu.Unlock()

	w.wg.Wait()
	w.pool.stop()

	w.logger.Info("Worker manager stopped")
}

// SubmitTask submits a task for execution by the specified tenant.
// If the tenant's queue is full or the tenant is shutting down, the task is
// dropped and an error is returned. This method is non-blocking and safe to
// call from multiple goroutines.
func (w *WorkerManager) SubmitTask(tenantID uuid.UUID, task Task) error {
	w.tenantsMu.RLock()
	state, ok := w.tenants[tenantID]
	w.tenantsMu.RUnlock()

	if !ok {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	select {
	case state.taskQueue <- task:
		return nil
	case <-state.ctx.Done():
		return fmt.Errorf("tenant %s is shutting down", tenantID)
	default:
		return fmt.Errorf("tenant queue full")
	}
}

// GetActiveTenants returns the IDs of all currently active tenants.
func (w *WorkerManager) GetActiveTenants() []uuid.UUID {
	w.tenantsMu.RLock()
	defer w.tenantsMu.RUnlock()

	tenants := make([]uuid.UUID, 0, len(w.tenants))
	for id := range w.tenants {
		tenants = append(tenants, id)
	}
	return tenants
}

// tenantRefresher periodically updates the list of active tenants.
// Runs in its own goroutine and exits when the manager stops.
func (w *WorkerManager) tenantRefresher() {
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

// refreshTenants updates the internal tenant state to match the current active tenants.
// It adds new tenants, adjusts worker counts for existing tenants on limit change,
// and removes inactive ones. Acquires tenantsMu itself.
func (w *WorkerManager) refreshTenants() error {
	active, err := w.provider.GetActive(w.ctx)
	if err != nil {
		return err
	}

	w.tenantsMu.Lock()
	defer w.tenantsMu.Unlock()

	// See Stop(): this check, performed under tenantsMu, prevents wg.Add
	// (via createTenant/setWorkerCount) from racing with wg.Wait.
	if w.stopping.Load() {
		return nil
	}

	current := make(map[uuid.UUID]struct{}, len(active))

	for _, t := range active {
		id := t.GetID()
		if id == uuid.Nil {
			continue
		}
		current[id] = struct{}{}

		limit := t.GetWorkerLimit()
		if limit <= 0 {
			limit = 3 // safe default if provider returns invalid value
		}

		if state, ok := w.tenants[id]; ok {
			w.setWorkerCount(state, limit)
			continue
		}

		w.createTenant(id, limit)
	}

	// Remove tenants that are no longer active.
	for id, state := range w.tenants {
		if _, exists := current[id]; !exists {
			state.cancel()
			delete(w.tenants, id)
		}
	}

	return nil
}

// createTenant initializes a new tenant with the given ID and worker limit.
// Caller must hold tenantsMu.
func (w *WorkerManager) createTenant(id uuid.UUID, limit int) {
	ctx, cancel := context.WithCancel(w.ctx)

	// Queue buffer size is 4x the initial worker limit to absorb bursts.
	// Not resized on later limit changes.
	state := &tenantState{
		id:        id,
		taskQueue: make(chan Task, limit*4),
		ctx:       ctx,
		cancel:    cancel,
	}

	w.tenants[id] = state
	w.setWorkerCount(state, limit)
}

// setWorkerCount (re)starts the tenant's worker generation with exactly
// `limit` workers. The previous generation (if any) is cancelled via
// genCancel: those workers finish the task they're currently holding
// (executeTask stops waiting on genCtx.Done but the task keeps running in
// the pool to completion) and then exit on their next loop iteration.
//
// taskQueue is shared and unbuffered-safe across generations since it is
// never closed. Caller must hold tenantsMu.
func (w *WorkerManager) setWorkerCount(state *tenantState, limit int) {
	if state.limit == limit && state.genCancel != nil {
		return
	}

	if state.genCancel != nil {
		state.genCancel()
	}

	genCtx, genCancel := context.WithCancel(state.ctx)
	state.genCancel = genCancel
	state.limit = limit

	for workerID := 0; workerID < limit; workerID++ {
		w.wg.Add(1)
		go w.workerLoop(state, genCtx, workerID)
	}
}

// workerLoop is the main processing loop for a tenant worker generation.
// It exits when genCtx is cancelled (tenant removed or limit changed).
func (w *WorkerManager) workerLoop(state *tenantState, genCtx context.Context, workerID int) {
	defer w.wg.Done()

	for {
		select {
		case <-genCtx.Done():
			return
		case task := <-state.taskQueue:
			w.executeTask(state, genCtx, workerID, task)
		}
	}
}

// executeTask submits the task to the global pool and blocks until the pool
// has actually finished executing it (or genCtx is cancelled). This is what
// makes "limit workers" mean "at most `limit` of this tenant's tasks running
// in the pool concurrently" - the previous inflight-semaphore design released
// its slot right after enqueueing, so it enforced nothing.
//
// task.Complete is wrapped so it fires exactly once regardless of whether the
// pool ever runs the task (addTask can fail before the task reaches the pool).
func (w *WorkerManager) executeTask(state *tenantState, genCtx context.Context, workerID int, task Task) {
	done := make(chan struct{})
	originalComplete := task.Complete
	task.Complete = func() {
		if originalComplete != nil {
			originalComplete()
		}
		close(done)
	}

	if err := w.pool.addTask(task); err != nil {
		w.logger.Warn("Failed to submit task to pool",
			slog.String("tenant_id", state.id.String()),
			slog.Int("worker_id", workerID),
			slog.Any("error", err))
		task.Complete() // pool will never run it, so it will never call Complete itself
		return
	}

	select {
	case <-done:
	case <-genCtx.Done():
		// Generation is being torn down (limit change or tenant removal).
		// The task keeps running in the pool and its Complete will still
		// fire, but this worker stops waiting so it can exit promptly.
	}
}
