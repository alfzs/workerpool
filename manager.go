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
// All fields are protected by the tenant's context or atomic operations.
type tenantState struct {
	id uuid.UUID

	// taskQueue is a channel for incoming tasks.
	// Each task is sent directly to the queue for processing.
	taskQueue chan Task

	// limit is the current concurrency limit for this tenant.
	// Updated atomically when tenant configuration changes.
	limit atomic.Int32

	// inflight is a counting semaphore implemented as a buffered channel.
	// It limits the number of concurrently executing tasks.
	inflight chan struct{}

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

	w.tenantsMu.Lock()
	for _, t := range w.tenants {
		t.cancel()
		close(t.taskQueue)
	}
	w.tenantsMu.Unlock()

	w.wg.Wait()
	w.pool.stop()

	w.logger.Info("Worker manager stopped")
}

// SubmitTask submits a task for execution by the specified tenant.
// If the tenant's queue is full, the task is dropped and an error is returned.
// This method is non-blocking and safe to call from multiple goroutines.
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
// It adds new tenants, updates limits for existing tenants, and removes inactive ones.
// Must be called with tenantsMu held for writing.
func (w *WorkerManager) refreshTenants() error {
	active, err := w.provider.GetActive(w.ctx)
	if err != nil {
		return err
	}

	w.tenantsMu.Lock()
	defer w.tenantsMu.Unlock()

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
			// Update limit for existing tenant
			state.limit.Store(int32(limit))
			continue
		}

		// Create new tenant
		w.createTenant(id, limit)
	}

	// Remove tenants that are no longer active
	for id, state := range w.tenants {
		if _, exists := current[id]; !exists {
			state.cancel()
			close(state.taskQueue)
			delete(w.tenants, id)
		}
	}

	return nil
}

// createTenant initializes a new tenant with the given ID and worker limit.
// It creates the tenant's queue, inflight semaphore, and starts the required number of workers.
func (w *WorkerManager) createTenant(id uuid.UUID, limit int) {
	ctx, cancel := context.WithCancel(w.ctx)

	// Queue buffer size is 4x the worker limit to absorb bursts
	// This is a reasonable default that prevents most drops while bounding memory
	state := &tenantState{
		id:        id,
		taskQueue: make(chan Task, limit*4),
		inflight:  make(chan struct{}, limit),
		ctx:       ctx,
		cancel:    cancel,
	}

	state.limit.Store(int32(limit))
	w.tenants[id] = state

	// Start exactly 'limit' workers for this tenant
	for workerID := 0; workerID < limit; workerID++ {
		w.wg.Add(1)
		go w.workerLoop(state, workerID)
	}
}

// workerLoop is the main processing loop for a tenant worker.
// It waits for tasks on the queue, acquires an inflight slot, and executes the task.
// The loop exits when the tenant context is cancelled.
func (w *WorkerManager) workerLoop(state *tenantState, workerID int) {
	defer w.wg.Done()

	for {
		select {
		case <-state.ctx.Done():
			return

		case task, ok := <-state.taskQueue:
			if !ok {
				return
			}

			// Acquire inflight slot before executing
			select {
			case state.inflight <- struct{}{}:
			case <-state.ctx.Done():
				return
			}

			w.executeTask(state, workerID, task)
		}
	}
}

// executeTask handles the actual task execution for a tenant worker.
// It submits the task to the global pool and manages cleanup.
// The inflight slot is released when this function returns.
func (w *WorkerManager) executeTask(state *tenantState, workerID int, task Task) {
	// Release inflight slot when done
	defer func() {
		<-state.inflight
	}()

	// Submit to global pool
	if err := w.pool.addTask(task); err != nil {
		w.logger.Warn("Failed to submit task to pool",
			slog.String("tenant_id", state.id.String()),
			slog.Int("worker_id", workerID),
			slog.Any("error", err))
	}
}
