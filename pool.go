package workerpool

import (
	"container/heap"
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

// TaskPriority определяет приоритет задачи.
type TaskPriority int

const (
	PriorityLow    TaskPriority = 0
	PriorityNormal TaskPriority = 1
	PriorityHigh   TaskPriority = 2
)

// Task представляет задачу для выполнения в пуле.
type Task struct {
	Ctx      context.Context
	TaskID   uuid.UUID
	TenantID uuid.UUID
	Executor taskExecutor
	Priority TaskPriority
	Complete func()
}

// priorityQueue реализует интерфейс heap.Interface для очереди с приоритетами.
type priorityQueue []*Task

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	return pq[i].TaskID.String() < pq[j].TaskID.String()
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*Task)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}

type pool struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
	config Config

	highQueue   chan *Task
	normalQueue chan *Task
	lowQueue    chan *Task

	overflowPQ *priorityQueue
	pqMutex    sync.Mutex

	wg       sync.WaitGroup
	stopping atomic.Bool
	closed   atomic.Bool

	runningTasks sync.Map

	retryPredicate RetryPredicate
	maxAttempts    int
}

type PoolParams struct {
	Ctx            context.Context
	Logger         *slog.Logger
	Config         Config
	RetryPredicate RetryPredicate
}

func newPool(p PoolParams) (*pool, error) {
	ctx, cancel := context.WithCancel(p.Ctx)

	predicate := p.RetryPredicate
	if predicate == nil {
		predicate = DefaultRetryPredicate
	}

	pool := &pool{
		ctx:            ctx,
		cancel:         cancel,
		logger:         p.Logger.With(slog.String("component", "worker_pool")),
		config:         p.Config,
		highQueue:      make(chan *Task, p.Config.HighPriorityQueueSize),
		normalQueue:    make(chan *Task, p.Config.NormalPriorityQueueSize),
		lowQueue:       make(chan *Task, p.Config.LowPriorityQueueSize),
		overflowPQ:     &priorityQueue{},
		retryPredicate: predicate,
		maxAttempts:    p.Config.RetryPolicy.Attempts.Count,
	}
	heap.Init(pool.overflowPQ)

	return pool, nil
}

func (p *pool) start() {
	totalWorkers := p.config.PoolSize.High + p.config.PoolSize.Normal + p.config.PoolSize.Low
	for i := 0; i < totalWorkers; i++ {
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
	SetActiveWorkers(int64(totalWorkers))
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
		p.logger.Info("worker pool stopped gracefully")
	case <-time.After(p.config.GracefulTimeout):
		p.logger.Error("timeout stopping worker pool")
	}
}

func (p *pool) addTask(task *Task) error {
	if p.stopping.Load() {
		return fmt.Errorf("pool is stopping")
	}

	if !p.tryMarkTaskRunning(task.TenantID, task.TaskID) {
		return fmt.Errorf("task %s is already running for tenant %s", task.TaskID, task.TenantID)
	}

	originalComplete := task.Complete
	task.Complete = func() {
		p.unmarkTaskRunning(task.TenantID, task.TaskID)
		if originalComplete != nil {
			originalComplete()
		}
	}

	var ch chan *Task
	switch task.Priority {
	case PriorityHigh:
		ch = p.highQueue
	case PriorityNormal:
		ch = p.normalQueue
	default:
		ch = p.lowQueue
	}

	select {
	case ch <- task:
		IncTasksTotal()
		SetQueuedTasks(int64(len(p.highQueue) + len(p.normalQueue) + len(p.lowQueue) + p.overflowPQ.Len()))
		return nil
	default:
		p.pqMutex.Lock()
		heap.Push(p.overflowPQ, task)
		p.pqMutex.Unlock()
		IncTasksTotal()
		SetQueuedTasks(int64(len(p.highQueue) + len(p.normalQueue) + len(p.lowQueue) + p.overflowPQ.Len()))
		return nil
	}
}

func (p *pool) worker(id int) {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		task := p.dequeueTask()
		if task == nil {
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}

		p.runTask(task, id)
		SetQueuedTasks(int64(len(p.highQueue) + len(p.normalQueue) + len(p.lowQueue) + p.overflowPQ.Len()))
	}
}

func (p *pool) dequeueTask() *Task {
	select {
	case task := <-p.highQueue:
		return task
	default:
	}

	select {
	case task := <-p.normalQueue:
		return task
	default:
	}

	p.pqMutex.Lock()
	if p.overflowPQ.Len() > 0 {
		task := heap.Pop(p.overflowPQ).(*Task)
		p.pqMutex.Unlock()
		return task
	}
	p.pqMutex.Unlock()

	select {
	case task := <-p.lowQueue:
		return task
	default:
	}

	return nil
}

func (p *pool) tryMarkTaskRunning(tenantID, taskID uuid.UUID) bool {
	tenantTasks, _ := p.runningTasks.LoadOrStore(tenantID, &sync.Map{})
	tasksMap := tenantTasks.(*sync.Map)

	_, loaded := tasksMap.LoadOrStore(taskID, struct{}{})
	return !loaded
}

func (p *pool) unmarkTaskRunning(tenantID, taskID uuid.UUID) {
	if tenantTasks, ok := p.runningTasks.Load(tenantID); ok {
		tasksMap := tenantTasks.(*sync.Map)
		tasksMap.Delete(taskID)
	}
}

func (p *pool) runTask(task *Task, workerID int) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("panic in task execution",
				slog.Any("recover", r),
				slog.String("stack", string(debug.Stack())))
			IncWorkerPanicsTotal()
		}
		if task.Complete != nil {
			safeComplete(task.Complete)
		}
	}()

	p.executeWithRetry(task, workerID)
}

func (p *pool) executeWithRetry(task *Task, workerID int) {
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
