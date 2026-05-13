package workerpool

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

type pool struct {
	logger *slog.Logger
	config Config

	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	queue *priorityHeap
	cond  *sync.Cond

	active atomic.Int64
	stop   atomic.Bool

	retry RetryPredicate

	rngSeed uint64
}

func newPool(logger *slog.Logger, cfg Config, retry RetryPredicate) *pool {
	ctx, cancel := context.WithCancel(context.Background())

	p := &pool{
		logger:  logger,
		config:  cfg,
		ctx:     ctx,
		cancel:  cancel,
		queue:   newPriorityHeap(5),
		retry:   retry,
		rngSeed: rand.Uint64(),
	}

	if p.retry == nil {
		p.retry = func(error) bool { return true }
	}

	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *pool) context() context.Context {
	return p.ctx
}

func (p *pool) Start() {
	for i := 0; i < p.config.PoolSize.Workers; i++ {
		go p.worker(i)
	}

	p.logger.Info("worker pool started")
}

func (p *pool) Stop() {
	if !p.stop.CompareAndSwap(false, true) {
		return
	}

	p.cancel()
	p.cond.Broadcast()

	timeout := time.After(p.config.GracefulTimeout)

	for p.active.Load() > 0 {
		select {
		case <-timeout:
			p.logger.Warn("shutdown timeout",
				slog.Int64("active", p.active.Load()),
			)
			return
		case <-time.After(50 * time.Millisecond):
		}
	}

	p.logger.Info("worker pool stopped")
}

func (p *pool) submit(task *PoolTask) error {
	if p.stop.Load() {
		return fmt.Errorf("pool stopped")
	}

	task.createdAt = time.Now()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.queue.Len() >= p.config.QueueSize {
		return fmt.Errorf("queue full")
	}

	task.effectivePriority = p.queue.compute(task)

	heap.Push(p.queue, task)
	p.cond.Signal()

	return nil
}

func (p *pool) worker(id int) {
	for {
		task := p.dequeue()
		if task == nil {
			return
		}

		p.active.Add(1)
		p.execute(task, id)
		p.active.Add(-1)
	}
}

func (p *pool) dequeue() *PoolTask {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.queue.Len() == 0 {
		if p.stop.Load() {
			return nil
		}
		p.cond.Wait()
	}

	return heap.Pop(p.queue).(*PoolTask)
}

func (p *pool) execute(task *PoolTask, workerID int) {
	defer func() {
		if task.OnComplete != nil {
			safe(task.OnComplete)
		}
	}()

	max := p.config.Retry.MaxAttempts
	if max <= 0 {
		max = 1
	}

	var err error

	for attempt := 1; attempt <= max; attempt++ {
		err = p.run(task, workerID)
		if err == nil {
			return
		}

		if _, ok := err.(*PanicError); ok {
			return
		}

		if task.Ctx.Err() != nil {
			return
		}

		if !p.retry(err) {
			return
		}

		if attempt == max {
			break
		}

		delay := p.backoff(attempt)

		select {
		case <-time.After(delay):
		case <-task.Ctx.Done():
			return
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *pool) run(task *PoolTask, workerID int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = capturePanic(task.TaskName, task.TenantID, workerID, r)
		}
	}()

	return task.Executor.Execute(task.Ctx, task.TenantID, workerID)
}

func (p *pool) backoff(attempt int) time.Duration {
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}

	delay := p.config.Retry.MinDelay * time.Duration(1<<uint(shift))
	if delay > p.config.Retry.MaxDelay {
		delay = p.config.Retry.MaxDelay
	}

	jitter := time.Duration(float64(delay) * 0.25)

	r := rand.New(rand.NewPCG(p.rngSeed, uint64(attempt)))

	return delay - jitter + time.Duration(r.Int64N(int64(jitter*2)))
}

func safe(fn func()) {
	defer func() {
		recover()
	}()
	fn()
}
