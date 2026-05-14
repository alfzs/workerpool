package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type pool struct {
	logger         *slog.Logger
	config         Config
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	cond           *sync.Cond
	scheduler      *drrScheduler
	wg             sync.WaitGroup
	activeCount    atomic.Int64
	stopped        atomic.Bool
	retryPredicate RetryPredicate
	rngMu          sync.Mutex
	rng            *rand.Rand
}

func newPool(logger *slog.Logger, config Config, retry RetryPredicate) *pool {
	ctx, cancel := context.WithCancel(context.Background())

	p := &pool{
		logger: logger.With(
			slog.String("component", "worker_pool"),
		),
		config: config,
		ctx:    ctx,
		cancel: cancel,
		scheduler: newDRRScheduler(
			config.DefaultQuantum,
			config.MaxTenantQueue,
		),
		retryPredicate: retry,
		rng: rand.New(
			rand.NewPCG(
				rand.Uint64(),
				rand.Uint64(),
			),
		),
	}

	p.cond = sync.NewCond(&p.mu)

	if p.retryPredicate == nil {
		p.retryPredicate = DefaultRetryPredicate
	}

	return p
}

func (p *pool) context() context.Context {
	return p.ctx
}

func (p *pool) start() {
	for i := 0; i < p.config.Workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

func (p *pool) stop() {
	if !p.stopped.CompareAndSwap(false, true) {
		return
	}

	p.cancel()

	p.cond.Broadcast()

	done := make(chan struct{})

	go func() {
		for p.activeCount.Load() > 0 {
			time.Sleep(50 * time.Millisecond)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(p.config.GracefulTimeout):
	}

	p.cond.Broadcast()

	p.wg.Wait()
}

func (p *pool) submit(task *PoolTask) error {
	if p.stopped.Load() {
		return fmt.Errorf("pool stopped")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.scheduler.enqueue(task); err != nil {
		return err
	}

	p.cond.Signal()

	return nil
}

func (p *pool) dequeue() *PoolTask {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		if p.stopped.Load() {
			return nil
		}

		task := p.scheduler.dequeue()
		if task != nil {
			return task
		}

		p.cond.Wait()
	}
}

func (p *pool) worker(workerID int) {
	defer p.wg.Done()

	for {
		task := p.dequeue()
		if task == nil {
			return
		}

		p.activeCount.Add(1)

		p.execute(task, workerID)

		p.activeCount.Add(-1)
	}
}

func (p *pool) execute(
	task *PoolTask,
	workerID int,
) {

	defer func() {
		if task.OnComplete != nil {
			safeCall(p.logger, task.OnComplete)
		}
	}()

	maxAttempts := p.config.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := p.executeSafely(task, workerID)

		if err == nil {
			return
		}

		lastErr = err

		if _, ok := err.(*PanicError); ok {
			return
		}

		if task.Ctx.Err() != nil {
			return
		}

		if !p.retryPredicate(err) {
			return
		}

		if attempt == maxAttempts {
			break
		}

		delay := p.backoff(attempt)

		select {
		case <-time.After(delay):
		case <-task.Ctx.Done():
			return
		}
	}

	p.logger.Error(
		"task failed",
		"task",
		task.TaskName,
		"error",
		lastErr,
	)
}

func (p *pool) executeSafely(
	task *PoolTask,
	workerID int,
) (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{
				TaskName: task.TaskName,
				WorkerID: workerID,
				Value:    r,
				Stack:    string(debug.Stack()),
			}
		}
	}()

	return task.Task.Execute(
		task.Ctx,
		task.TenantID,
		workerID,
	)
}

func (p *pool) backoff(attempt int) time.Duration {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()

	return calculateBackoff(
		p.rng,
		attempt,
		p.config.Retry.MinDelay,
		p.config.Retry.MaxDelay,
	)
}

func safeCall(
	logger *slog.Logger,
	fn func(),
) {

	defer func() {
		if r := recover(); r != nil {
			logger.Error(
				"panic in callback",
				"panic",
				r,
			)
		}
	}()

	fn()
}
