package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alfzs/backoff"
	"github.com/alfzs/tracing"
	"github.com/google/uuid"
)

// Task represents a unit of work to be executed by the worker pool.
// It contains all necessary context and metadata for execution.
type Task struct {
	// Ctx is the context for this task, including timeout and cancellation.
	Ctx context.Context

	// TaskID uniquely identifies this task instance.
	TaskID uuid.UUID

	// TenantID identifies which tenant this task belongs to.
	TenantID uuid.UUID

	// Executor is the function that performs the actual work.
	Executor taskExecutor

	// Complete is an optional callback that is called after task execution
	// (whether successful or failed). It is guaranteed to be called exactly once.
	Complete func()
}

// pool is a fixed-size worker pool that executes tasks with retry logic.
// It provides a global execution capacity shared across all tenants.
type pool struct {
	logger *slog.Logger
	config Config

	// taskChan is the buffered channel for incoming tasks.
	taskChan chan Task

	// closeMu guards the invariant "no send on taskChan happens after it is
	// closed": addTask holds RLock while checking stopping and sending;
	// stop() holds Lock while closing. This prevents send-on-closed-channel
	// panics under concurrent addTask/stop.
	closeMu sync.RWMutex

	// workerCount is the number of concurrent workers.
	workerCount int

	// maxAttempts is the number of retry attempts per task. Treated as 1 if
	// configured as <= 0, since 0 attempts would mean Execute is never called.
	maxAttempts int

	wg       sync.WaitGroup
	stopping atomic.Bool
}

// PoolParams contains parameters for creating a new pool.
type PoolParams struct {
	Logger *slog.Logger
	Config Config
}

// newPool creates a new worker pool with the given configuration.
// The pool is initially stopped; call Start() to begin processing.
func newPool(p PoolParams) (*pool, error) {
	maxAttempts := p.Config.RetryPolicy.Attempts.Count
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	return &pool{
		logger:      p.Logger.With(slog.String("component", "worker_pool")),
		config:      p.Config,
		workerCount: p.Config.WorkerCount,
		maxAttempts: maxAttempts,
		taskChan:    make(chan Task, p.Config.TaskQueueSize),
	}, nil
}

// start launches the worker goroutines.
// It must be called before any tasks can be processed.
func (p *pool) start() {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		workerID := i
		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.logger.Error("Panic in worker",
						slog.Any("panic", r),
						slog.String("stack", string(debug.Stack())))
				}
				p.wg.Done()
			}()
			p.worker(workerID)
		}()
	}
}

// stop initiates graceful shutdown of the pool.
// It closes the task channel and waits for workers to finish
// up to the configured GracefulTimeout.
func (p *pool) stop() {
	if !p.stopping.CompareAndSwap(false, true) {
		return
	}

	// Lock excludes any in-flight addTask (which holds RLock), so taskChan
	// is only closed once no goroutine can be mid-send on it.
	p.closeMu.Lock()
	close(p.taskChan)
	p.closeMu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("Worker pool stopped gracefully")
	case <-time.After(p.config.GracefulTimeout):
		p.logger.Error("Worker pool stop timeout")
	}
}

// addTask submits a task to the pool for execution.
// Returns an error if the pool is stopping or the queue is full.
func (p *pool) addTask(task Task) error {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()

	if p.stopping.Load() {
		return fmt.Errorf("pool stopping")
	}

	select {
	case p.taskChan <- task:
		return nil

	default:
		return fmt.Errorf("task queue full")
	}
}

// worker is the main processing loop for a pool worker.
// It reads tasks from the channel and executes them.
func (p *pool) worker(id int) {
	for task := range p.taskChan {
		p.runTask(task, id)
	}
}

// runTask executes a single task with panic recovery and completion callback.
// This function guarantees that task.Complete is called exactly once.
func (p *pool) runTask(task Task, workerID int) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("Panic in task",
				slog.Any("recover", r),
				slog.String("stack", string(debug.Stack())))
		}

		// Guaranteed single execution of Complete callback.
		if task.Complete != nil {
			task.Complete()
		}
	}()

	p.executeWithRetry(task, workerID)
}

// executeWithRetry attempts to execute a task with retries and exponential backoff.
// It respects context cancellation and pool shutdown signals.
func (p *pool) executeWithRetry(task Task, workerID int) {
	ctxWithTrace := tracing.EnsureTraceID(task.Ctx)

	var lastErr error
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		err := task.Executor.Execute(ctxWithTrace, task.TenantID, workerID)
		if err == nil {
			return // Success
		}

		lastErr = err

		if attempt == p.maxAttempts {
			break // last attempt failed, no point sleeping before reporting
		}

		delay := backoff.CalculateExponentialBackoff(
			attempt,
			p.config.RetryPolicy.Attempts.MinDelay,
			p.config.RetryPolicy.Attempts.MaxDelay,
		)

		select {
		case <-time.After(delay):
		case <-task.Ctx.Done():
			return // Task cancelled
		}
	}

	p.logger.Error("Task failed after retries",
		slog.String("tenant_id", task.TenantID.String()),
		slog.Any("error", lastErr))
}
