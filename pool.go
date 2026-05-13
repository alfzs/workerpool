// pool.go
package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alfzs/tracing"
	"github.com/google/uuid"
)

type Task struct {
	Ctx      context.Context
	TaskID   uuid.UUID
	TenantID uuid.UUID
	Executor taskExecutor
	Complete func()
}

type pool struct {
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *slog.Logger
	config      Config
	taskChan    chan Task
	workerCount int
	maxAttempts int
	wg          sync.WaitGroup
	stopping    atomic.Bool
	closed      atomic.Bool
	// retryPredicate — пользовательская логика проверки ретраев
	retryPredicate RetryPredicate
}

type PoolParams struct {
	Ctx    context.Context
	Logger *slog.Logger
	Config Config
	// RetryPredicate — опциональная кастомная логика проверки ретраев.
	// Если nil, используется DefaultRetryPredicate.
	RetryPredicate RetryPredicate
}

func newPool(p PoolParams) (*pool, error) {
	ctx, cancel := context.WithCancel(p.Ctx)

	// Устанавливаем предикат по умолчанию, если не передан
	predicate := p.RetryPredicate
	if predicate == nil {
		predicate = DefaultRetryPredicate
	}

	return &pool{
		ctx:            ctx,
		cancel:         cancel,
		logger:         p.Logger.With(slog.String("component", "worker_pool")),
		config:         p.Config,
		workerCount:    p.Config.PoolSize.Normal,
		maxAttempts:    p.Config.RetryPolicy.Attempts.Count,
		taskChan:       make(chan Task, p.Config.TaskQueueSize),
		retryPredicate: predicate,
	}, nil
}

func (p *pool) start() {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		workerID := i
		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.logger.Error("panic in pool worker",
						slog.Any("panic", r),
						slog.String("stack", string(debug.Stack())))
					IncWorkerPanicsTotal()
				}
				p.wg.Done()
			}()
			p.worker(workerID)
		}()
	}
}

func (p *pool) stop() {
	if !p.stopping.CompareAndSwap(false, true) {
		return
	}

	p.cancel()
	p.closed.Store(true)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(p.config.GracefulTimeout):
		p.logger.Error("timeout stopping worker pool")
	}
}

func (p *pool) addTask(task Task) error {
	if p.stopping.Load() {
		return fmt.Errorf("pool is stopping")
	}

	if p.closed.Load() {
		return fmt.Errorf("pool is closed")
	}

	select {
	case p.taskChan <- task:
		IncTasksTotal()
		SetQueuedTasks(int64(len(p.taskChan)))
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	default:
		IncTasksDroppedTotal()
		return fmt.Errorf("task queue is full")
	}
}

func (p *pool) worker(id int) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case task, ok := <-p.taskChan:
			if !ok {
				return
			}
			p.runTask(task, id)
			SetQueuedTasks(int64(len(p.taskChan)))
		}
	}
}

func (p *pool) runTask(task Task, workerID int) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("panic in task execution",
				slog.Any("recover", r),
				slog.String("stack", string(debug.Stack())))
			IncWorkerPanicsTotal()
		}
		safeComplete(task.Complete)
	}()

	p.executeWithRetry(task, workerID)
}

func calculateJitteredDelay(attempt int, minDelay, maxDelay time.Duration) time.Duration {
	delay := minDelay
	for i := 1; i < attempt && delay < maxDelay; i++ {
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}

	return time.Duration(rand.Int63n(int64(delay)))
}

func (p *pool) executeWithRetry(task Task, workerID int) {
	ctxWithTrace := tracing.EnsureTraceID(task.Ctx)
	ctxWithAttempt := context.WithValue(ctxWithTrace, ctxKeyAttempt{}, 0)

	var lastErr error
	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		ctxWithAttempt = context.WithValue(ctxWithTrace, ctxKeyAttempt{}, attempt)

		startTime := time.Now()
		err := task.Executor.Execute(ctxWithAttempt, task.TenantID, workerID)
		duration := time.Since(startTime)
		ObserveTaskDuration(duration)

		if err == nil {
			return
		}

		// Проверяем, можно ли ретраить
		if !p.retryPredicate(err) {
			p.logger.Warn("non-retryable error, stopping attempts",
				slog.String("task_id", task.TaskID.String()),
				slog.String("tenant_id", task.TenantID.String()),
				slog.Int("attempt", attempt),
				slog.Any("error", err))
			IncTasksFailedTotal()
			return
		}

		lastErr = err
		IncTasksRetryTotal()

		if attempt == p.maxAttempts {
			break
		}

		delay := calculateJitteredDelay(
			attempt,
			p.config.RetryPolicy.Attempts.MinDelay,
			p.config.RetryPolicy.Attempts.MaxDelay,
		)
		ObserveRetryDelay(delay)

		select {
		case <-time.After(delay):
		case <-task.Ctx.Done():
			return
		case <-p.ctx.Done():
			return
		}
	}

	p.logger.Error("task failed after all attempts",
		slog.String("tenant_id", task.TenantID.String()),
		slog.String("task_id", task.TaskID.String()),
		slog.Int("attempts", p.maxAttempts),
		slog.Any("error", lastErr))
	IncTasksFailedTotal()
}

func safeComplete(fn func()) {
	if fn == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in task completion callback",
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())))
		}
	}()

	fn()
}
