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

	"github.com/alfzs/backoff"
	"github.com/alfzs/tracing"
	"github.com/google/uuid"
)

const (
	defaultTenantWorkerCount = 3
)

type WorkerManager struct {
	Ctx             context.Context
	Logger          *slog.Logger
	TenantProvider  tenantProvider
	Config          Config
	WorkerCount     int
	pool            *pool
	executors       map[uuid.UUID]taskExecutor
	executorsMu     sync.RWMutex
	timers          map[uuid.UUID]*time.Timer
	timersMu        sync.RWMutex
	activeTasks     map[string]context.CancelFunc
	activeTasksMu   sync.RWMutex
	tenantLimits    map[uuid.UUID]int
	tenantCounters  map[uuid.UUID]int
	tenantTaskQueue map[uuid.UUID][]Task // Очередь задач для каждого тенанта, если воркеров меньше задач
	limitsMu        sync.RWMutex
	stopChan        chan struct{}
	wg              sync.WaitGroup
	starting        atomic.Bool
	stopping        atomic.Bool
}

type WorkerManagerParams struct {
	Ctx            context.Context
	Logger         *slog.Logger
	TenantProvider tenantProvider
	Config         Config
	WorkerCount    int
}

// Task представляет задачу для выполнения
type Task struct {
	Ctx      context.Context
	TaskID   uuid.UUID
	TenantID uuid.UUID
	WorkerID int
	Executor taskExecutor
	Complete func()
}

type Tenant interface {
	GetID() uuid.UUID
	GetWorkerLimit() int
}

// tenantProvider интерфейс поставщика списка тенантов
type tenantProvider interface {
	GetActive(ctx context.Context) ([]Tenant, error)
}

// taskExecutor интерфейс для выполнения задач
type taskExecutor interface {
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error
}

// NewWorkerManager
func NewWorkerManager(p WorkerManagerParams) (*WorkerManager, error) {
	pool, err := newPool(Params{
		Ctx:     p.Ctx,
		Logger:  p.Logger,
		Configs: p.Config})
	if err != nil {
		return nil, err
	}

	return &WorkerManager{
		Ctx:             p.Ctx,
		Logger:          p.Logger.With(slog.String("component", "worker_manager")),
		TenantProvider:  p.TenantProvider,
		Config:          p.Config,
		WorkerCount:     p.WorkerCount,
		pool:            pool,
		executors:       make(map[uuid.UUID]taskExecutor),
		timers:          make(map[uuid.UUID]*time.Timer),
		activeTasks:     make(map[string]context.CancelFunc),
		tenantLimits:    make(map[uuid.UUID]int),
		tenantCounters:  make(map[uuid.UUID]int),
		tenantTaskQueue: make(map[uuid.UUID][]Task),
		stopChan:        make(chan struct{}),
	}, nil
}

func (w *WorkerManager) Start() {
	op := "start"

	// Атомарно устанавливаем флаг запуска
	if !w.starting.CompareAndSwap(false, true) {
		w.Logger.Info("Worker manager already in starting state", slog.String("op", op))
		return
	}

	w.Logger.Info("Starting worker manager", slog.String("op", op))

	if err := w.initTenantLimits(); err != nil {
		w.Logger.Error("Failed to initialize tenant limits, using defaults",
			slog.Any("error", err),
			slog.String("op", op))

		w.limitsMu.Lock()
		w.tenantLimits = make(map[uuid.UUID]int)
		w.tenantCounters = make(map[uuid.UUID]int)
		w.limitsMu.Unlock()
	}

	w.pool.start()
	w.wg.Add(1)
	go w.taskScheduler()
}

func (w *WorkerManager) initTenantLimits() error {
	op := "init_tenant_limits"

	tenants, err := w.TenantProvider.GetActive(w.Ctx)
	if err != nil {
		return err
	}

	w.limitsMu.Lock()
	defer w.limitsMu.Unlock()

	for _, tenant := range tenants {
		tenantID := tenant.GetID()
		if tenantID == uuid.Nil {
			continue
		}

		limit := w.getTenantWorkerLimit(tenant)
		if limit <= 0 {
			return fmt.Errorf("%s: failed set worker limit = %d", op, limit)
		}

		w.tenantLimits[tenant.GetID()] = limit
		w.tenantCounters[tenant.GetID()] = 0

	}

	return nil
}

// GetTenantWorkerLimit возвращает лимит воркеров для тенанта
func (w *WorkerManager) getTenantWorkerLimit(t Tenant) int {
	op := "get_tenant_worker_limit"

	// WARN: этого кейса быть не должно
	if t == nil {
		w.Logger.Error("nil tenant provided, using default worker limit",
			slog.String("op", op),
			slog.String("source", "default value"),
		)
		return defaultTenantWorkerCount
	}

	// Берем значение из БД
	limit := t.GetWorkerLimit()
	source := "tenant DB value"

	// Если в настройках пула установлено, то используем его
	if w.WorkerCount > 0 {
		limit = w.WorkerCount
		source = "worker manager config"
		// Fallback
	} else if limit <= 0 {
		limit = defaultTenantWorkerCount
		source = "default value"
	}

	w.Logger.Info("Set tenant worker limit",
		slog.String("tenant_id", t.GetID().String()),
		slog.String("op", op),
		slog.Int("limit", limit),
		slog.String("source", source),
	)
	return limit
}

func (w *WorkerManager) Stop() {
	op := "stop"

	// Атомарно устанавливаем флаг остановки
	if !w.stopping.CompareAndSwap(false, true) {
		w.Logger.Info("Worker manager already in stopping state", slog.String("op", op))
		return
	}
	w.Logger.Info("Starting worker manager shutdown", slog.String("op", op))

	// Закрываем канал остановки (для taskScheduler)
	close(w.stopChan)

	// Останавливаем все таймеры и предотвращаем их перезапуск
	w.timersMu.Lock()
	w.executorsMu.Lock() // Блокируем регистрацию новых задач
	for id, timer := range w.timers {
		if !timer.Stop() {
			// Таймер уже сработал или был остановлен
			select {
			case <-timer.C: // Дренируем канал если нужно
			default:
			}
		}
		delete(w.timers, id)
	}
	w.executorsMu.Unlock()
	w.timersMu.Unlock()

	// Останавливаем пул воркеров
	w.pool.stop()

	// Принудительная отмена оставшихся задач
	w.activeTasksMu.Lock()
	for key, cancel := range w.activeTasks {
		cancel()
		delete(w.activeTasks, key)
	}
	w.activeTasksMu.Unlock()

	// Ожидаем завершения активных задач с таймаутом
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.Logger.Info("All tasks completed successfully", slog.String("op", op))
	case <-time.After(w.Config.GracefulTimeout):
		w.Logger.Warn("Shutdown timed out, canceling remaining tasks", slog.String("op", op))
	}

	// Очищаем все структуры данных
	w.cleanupResources()
	w.Logger.Info("Worker manager fully stopped", slog.String("op", op))
}

func (w *WorkerManager) cleanupResources() {
	// Очистка executors
	w.executorsMu.Lock()
	w.executors = make(map[uuid.UUID]taskExecutor)
	w.executorsMu.Unlock()

	// Очистка tenant-related данных
	w.limitsMu.Lock()
	w.tenantLimits = make(map[uuid.UUID]int)
	w.tenantCounters = make(map[uuid.UUID]int)
	for tenantID := range w.tenantTaskQueue {
		w.tenantTaskQueue[tenantID] = nil // Освобождаем память
	}
	w.tenantTaskQueue = make(map[uuid.UUID][]Task)
	w.limitsMu.Unlock()

	// Очистка активных задач
	w.activeTasksMu.Lock()
	w.activeTasks = make(map[string]context.CancelFunc)
	w.activeTasksMu.Unlock()
}

func (w *WorkerManager) RegisterScheduledTask(exec taskExecutor, interval time.Duration, taskID uuid.UUID) error {
	op := "register_scheduled_task"

	log := w.Logger.With(slog.String("task_id", taskID.String()))

	if exec == nil {
		return fmt.Errorf("task executor is nil")
	}
	if taskID == uuid.Nil {
		return fmt.Errorf("task id is nil")
	}
	if interval == 0 {
		return fmt.Errorf("task interval is nil")
	}

	w.executorsMu.Lock()
	defer w.executorsMu.Unlock()

	if _, ok := w.executors[taskID]; ok {
		log.Warn("Task already registered", slog.String("op", op))
		return nil
	}

	// jitter первого запуска
	jitter := backoff.CalculateExponentialBackoff(
		rand.Intn(10)+1,
		w.Config.RetryPolicy.Jitter.MinDelay,
		w.Config.RetryPolicy.Jitter.MaxDelay,
	)
	initialDelay := interval + jitter

	log.Info("Registering new executor",
		slog.Duration("initial_interval", initialDelay),
		slog.Duration("interval", interval),
		slog.String("op", op))

	var timerCallback func()
	timerCallback = func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("Recovered from panic in timer callback",
					slog.Any("recover", r),
					slog.String("stack", string(debug.Stack())),
					slog.String("op", op))
			}
		}()

		w.triggerTask(taskID)

		w.timersMu.Lock()
		defer w.timersMu.Unlock()

		if timer, ok := w.timers[taskID]; ok {
			timer.Stop()
			select {
			case <-timer.C:
			default:
			}
		}
		w.timers[taskID] = time.AfterFunc(interval, timerCallback)
	}

	w.executors[taskID] = exec

	w.timersMu.Lock()
	defer w.timersMu.Unlock()
	if oldTimer, ok := w.timers[taskID]; ok {
		oldTimer.Stop()
	}

	w.timers[taskID] = time.AfterFunc(initialDelay, timerCallback)

	return nil
}

func (w *WorkerManager) taskScheduler() {
	defer w.wg.Done()

	w.executorsMu.RLock()
	for taskID := range w.executors {
		w.triggerTask(taskID)
	}
	w.executorsMu.RUnlock()

	for {
		select {
		case <-w.stopChan:
			return
		case <-w.Ctx.Done():
			return
		}
	}
}

func (w *WorkerManager) triggerTask(taskID uuid.UUID) {
	op := "trigger_task"

	log := w.Logger.With(slog.String("task_id", taskID.String()))

	w.executorsMu.RLock()
	executor, ok := w.executors[taskID]
	w.executorsMu.RUnlock()

	if !ok || executor == nil {
		log.Warn("Executor not found or nil", slog.String("op", op))
		return
	}

	tenants, err := w.TenantProvider.GetActive(w.Ctx)
	if err != nil {
		log.Error("Failed to get active tenants",
			slog.Any("error", err),
			slog.String("op", op),
		)

		return
	}
	if len(tenants) == 0 {
		log.Warn("No active tenants found, skip task", slog.String("op", op))

		return
	}

	for _, tenant := range tenants {
		tenantID := tenant.GetID()
		if tenantID == uuid.Nil {
			continue
		}

		key := makeTaskKey(taskID, tenantID)

		w.limitsMu.Lock()
		if _, ok := w.tenantLimits[tenantID]; !ok {
			limit := w.getTenantWorkerLimit(tenant)
			w.tenantLimits[tenantID] = limit
			w.tenantCounters[tenantID] = 0
		}
		limit := w.tenantLimits[tenantID]
		w.limitsMu.Unlock()

		w.activeTasksMu.RLock()
		_, ok := w.activeTasks[key]
		w.activeTasksMu.RUnlock()

		if ok {
			log.Info("Task already running",
				slog.String("tenant_id", tenantID.String()),
				slog.String("op", op))
			continue
		}

		if w.Ctx.Err() != nil {
			continue
		}

		// INFO: trace_id задачи tenants
		ctxWithTrace := tracing.EnsureTraceID(w.Ctx)
		ctx, cancel := context.WithCancel(ctxWithTrace)

		task := Task{
			TaskID:   taskID,
			TenantID: tenantID,
			Executor: &tenantTaskExecutor{
				executor:    executor,
				taskContext: ctx,
			},
			Ctx:      ctx,
			Complete: completedTask(w, tenantID, key),
		}

		w.activeTasksMu.Lock()
		w.activeTasks[key] = cancel
		w.activeTasksMu.Unlock()

		w.limitsMu.Lock()
		// Добавляем задачу в очередь тенанта
		w.tenantTaskQueue[tenantID] = append(w.tenantTaskQueue[tenantID], task)
		// Пытаемся запустить задачи из очереди
		w.processTenantQueue(tenantID, limit)
		w.limitsMu.Unlock()
	}
}

func (w *WorkerManager) processTenantQueue(tenantID uuid.UUID, limit int) {
	// Пока есть свободные воркеры и задачи в очереди
	for w.tenantCounters[tenantID] < limit && len(w.tenantTaskQueue[tenantID]) > 0 {
		task := w.tenantTaskQueue[tenantID][0]
		w.tenantTaskQueue[tenantID] = w.tenantTaskQueue[tenantID][1:]

		w.tenantCounters[tenantID]++
		key := makeTaskKey(task.TaskID, tenantID)

		w.activeTasksMu.Lock()
		w.activeTasks[key] = task.Complete
		w.activeTasksMu.Unlock()

		go w.handleAddTask(tenantID, task, key)
	}
}

func (w *WorkerManager) handleAddTask(tenantID uuid.UUID, task Task, key string) {
	op := "handle_add_task"

	err := w.pool.addTask(task)
	if err == nil {
		return // Успешная отправка — выходим
	}

	// Логирование ошибки
	w.Logger.Error("Failed to add task to pool",
		slog.Any("error", err),
		slog.String("op", op))

	// Освобождение ресурсов
	w.limitsMu.Lock()
	w.tenantCounters[tenantID]--
	w.limitsMu.Unlock()

	w.activeTasksMu.Lock()
	delete(w.activeTasks, key)
	w.activeTasksMu.Unlock()

	// Возврат задачи в очередь
	w.limitsMu.Lock()
	w.tenantTaskQueue[tenantID] = append([]Task{task}, w.tenantTaskQueue[tenantID]...)
	w.limitsMu.Unlock()
}

func completedTask(w *WorkerManager, tenantID uuid.UUID, key string) func() {
	return func() {
		w.limitsMu.Lock()
		defer w.limitsMu.Unlock()

		if w.tenantCounters[tenantID] > 0 {
			w.tenantCounters[tenantID]--
		}

		w.activeTasksMu.Lock()
		delete(w.activeTasks, key)
		w.activeTasksMu.Unlock()

		// После завершения задачи пытаемся обработать следующую из очереди
		w.processTenantQueue(tenantID, w.tenantLimits[tenantID])
	}
}

func makeTaskKey(taskID uuid.UUID, tenantID uuid.UUID) string {
	return fmt.Sprintf("%s:%s", taskID.String(), tenantID.String())
}
