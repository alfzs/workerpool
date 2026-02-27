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
)

// pool реализует workerpool
type pool struct {
	ctx         context.Context
	logger      *slog.Logger
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	taskChan    chan Task
	workerCount int
	maxAttempts int
	config      Config
	stopping    atomic.Bool
}

// Params
type Params struct {
	Ctx     context.Context
	Logger  *slog.Logger
	Configs Config
}

// newPool создает новый workerpool
func newPool(p Params) (*pool, error) {
	ctx, cancel := context.WithCancel(p.Ctx)

	return &pool{
		ctx:         ctx,
		cancel:      cancel,
		logger:      p.Logger.With(slog.String("component", "worker_pool")),
		config:      p.Configs,
		workerCount: p.Configs.Size.Normal,
		maxAttempts: p.Configs.RetryPolicy.Attempts.Count,
		taskChan:    make(chan Task, p.Configs.TaskQueueSize),
	}, nil
}

// start запускает воркеры
func (p *pool) start() {
	op := "start"

	p.logger.Info("Starting worker pool",
		slog.Int("workers", p.workerCount),
		slog.Int("queue_size", cap(p.taskChan)),
		slog.String("op", op))

	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		workerID := i
		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.logger.Error("Recovered from panic in worker goroutine",
						slog.Any("panic", r),
						slog.String("stack", string(debug.Stack())),
						slog.Int("worker_id", workerID),
						slog.String("op", op))
				}
			}()
			p.worker(workerID)
		}()
	}
}

// stop корректно останавливает пул
func (p *pool) stop() {
	op := "stop"

	if !p.stopping.CompareAndSwap(false, true) {
		return // Уже останавливается
	}

	p.logger.Info("Starting worker pool shutdown", slog.String("op", op))
	p.cancel() // Отменяем контекст, чтобы остановить все операции

	// Закрываем канал задач после отмены контекста
	close(p.taskChan)

	// Ждем завершения с таймаутом
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("Worker pool fully stopped", slog.String("op", op))
	case <-time.After(p.config.GracefulTimeout):
		p.logger.Error("Worker pool shutdown timed out", slog.String("op", op))
	}
}

// addTask добавляет задачу в очередь
func (p *pool) addTask(task Task) error {
	if p.stopping.Load() {
		return fmt.Errorf("pool is stopping - new tasks not accepted")
	}

	if task.Executor == nil {
		return fmt.Errorf("task executor is nil")
	}

	select {
	case p.taskChan <- task:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	default:
		return fmt.Errorf("task queue is full")
	}
}

// worker выполняет задачи из taskChan
func (p *pool) worker(id int) {
	op := "worker"
	log := p.logger.With(
		slog.String("trace_id", tracing.GetTraceID(p.ctx)),
		slog.Int("worker_id", id),
	)

	defer func() {
		if r := recover(); r != nil {
			log.Error("Recovered from panic inside worker",
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())),
				slog.String("op", op))
		}
		log.Info("Worker stopped", slog.String("op", op))
		p.wg.Done()
	}()

	log.Info("Worker started", slog.String("op", op))

	for task := range p.taskChan {
		if p.stopping.Load() || p.ctx.Err() != nil {
			log.Info("Stopping detected, breaking worker loop", slog.String("op", op))
			break
		}

		taskLog := log.With(
			slog.String("tenant_id", task.TenantID.String()),
			slog.String("task_id", task.TaskID.String()),
		)

		p.runTask(task, id, taskLog)
	}

	log.Info("Task channel closed, worker exiting", slog.String("op", op))
}

func (p *pool) runTask(task Task, id int, log *slog.Logger) {
	op := "run_task"

	defer func() {
		if r := recover(); r != nil {
			log.Error("Recovered from panic in task execution",
				slog.Any("recover", r),
				slog.String("stack", string(debug.Stack())),
				slog.String("op", op))
		}

		// Гарантированно вызываем Complete, даже при панике
		if task.Complete != nil {
			defer func() {
				if r := recover(); r != nil {
					log.Error("Recovered from panic in complete callback",
						slog.Any("recover", r),
						slog.String("stack", string(debug.Stack())),
						slog.String("op", op))
				}
			}()
			task.Complete()
		}
	}()

	p.executeWithRetry(task, id, log)
}

func (p *pool) executeWithRetry(task Task, workerID int, log *slog.Logger) {
	op := "execute_with_retry"

	defer func() {
		if task.Complete != nil {
			task.Complete()
		}
	}()

	if task.Ctx == nil || p.ctx == nil {
		log.Warn("Context is nil, skipping execution", slog.String("op", op))
		return
	}

	var lastErr error

	// Создаем контекст с trace ID один раз на всю задачу
	ctxWithTrace := tracing.EnsureTraceID(task.Ctx)
	taskLog := log.With(slog.String("trace_id", tracing.GetTraceID(ctxWithTrace)))

	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		attemptLog := taskLog.With(
			slog.Int("attempt", attempt),
			slog.Int("max_attempt", p.maxAttempts))

		err := task.Executor.Execute(ctxWithTrace, task.TenantID, workerID)
		if err == nil {
			attemptLog.Info("Task succeeded", slog.String("op", op))
			return
		}

		lastErr = err
		attemptLog.Warn("Task attempt failed",
			slog.Any("error", err),
			slog.String("op", op))

		// WARN: пауза перед повтором
		delay := backoff.CalculateExponentialBackoff(
			attempt,
			p.config.RetryPolicy.Attempts.MinDelay,
			p.config.RetryPolicy.Attempts.MaxDelay,
		)
		time.Sleep(delay)
	}

	if lastErr != nil {
		log.Error("Task failed after all attempts",
			slog.String("error", lastErr.Error()),
			slog.String("op", op))
	}
}
