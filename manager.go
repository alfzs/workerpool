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
}

type tenantProvider interface {
	GetActive(ctx context.Context) ([]Tenant, error)
}

type taskExecutor interface {
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error
}

// RegisteredTask представляет зарегистрированную задачу,
// которая будет выполняться для всех активных тенантов
type RegisteredTask struct {
	ID       uuid.UUID
	Name     string
	Executor taskExecutor
	Priority TaskPriority
	// TaskTimeout позволяет переопределить глобальный таймаут для этой задачи
	TaskTimeout *time.Duration
}

type TenantTaskStats struct {
	TenantID    uuid.UUID
	ActiveTasks int
	QueuedTasks int
}

type WorkerManager struct {
	logger   *slog.Logger
	provider tenantProvider
	config   Config
	pool     *pool

	ctx    context.Context
	cancel context.CancelFunc

	// Зарегистрированные задачи
	registeredTasks sync.Map // map[uuid.UUID]*RegisteredTask

	// Активные тенанты
	activeTenants sync.Map // map[uuid.UUID]Tenant

	// Для отслеживания выполняемых задач по тенантам и типам задач
	runningTenantTasks sync.Map // map[string]struct{} где ключ "tenantID:taskID"

	wg       sync.WaitGroup
	stopping atomic.Bool
}

type WorkerManagerParams struct {
	Logger         *slog.Logger
	TenantProvider tenantProvider
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
		config:   p.Config,
		pool:     pool,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (w *WorkerManager) Start() error {
	w.pool.start()

	// Первичная загрузка тенантов
	if err := w.refreshTenants(); err != nil {
		w.pool.stop()
		return fmt.Errorf("initial tenant refresh failed: %w", err)
	}

	// Запускаем фоновые процессы
	w.wg.Add(1)
	go w.refreshLoop()

	w.logger.Info("worker manager started",
		slog.Int("active_tenants", w.getActiveTenantCount()))

	return nil
}

func (w *WorkerManager) Stop() {
	if !w.stopping.CompareAndSwap(false, true) {
		return
	}

	w.logger.Info("stopping worker manager")
	w.cancel()
	w.wg.Wait()
	w.pool.stop()
	w.logger.Info("worker manager stopped")
}

// RegisterTask регистрирует задачу, которая будет выполняться для всех активных тенантов
func (w *WorkerManager) RegisterTask(task RegisteredTask) error {
	if task.ID == uuid.Nil {
		task.ID = uuid.New()
	}

	if task.Executor == nil {
		return fmt.Errorf("executor is required for task %s", task.Name)
	}

	if task.Priority == 0 {
		task.Priority = PriorityNormal
	}

	if _, exists := w.registeredTasks.Load(task.ID); exists {
		return fmt.Errorf("task with ID %s already registered", task.ID)
	}

	w.registeredTasks.Store(task.ID, &task)

	w.logger.Info("task registered",
		slog.String("task_id", task.ID.String()),
		slog.String("task_name", task.Name),
		slog.Int("priority", int(task.Priority)))

	return nil
}

// UnregisterTask удаляет зарегистрированную задачу
func (w *WorkerManager) UnregisterTask(taskID uuid.UUID) error {
	if _, exists := w.registeredTasks.Load(taskID); !exists {
		return fmt.Errorf("task with ID %s not found", taskID)
	}

	w.registeredTasks.Delete(taskID)

	w.logger.Info("task unregistered",
		slog.String("task_id", taskID.String()))

	return nil
}

// GetRegisteredTasks возвращает все зарегистрированные задачи
func (w *WorkerManager) GetRegisteredTasks() []RegisteredTask {
	var tasks []RegisteredTask
	w.registeredTasks.Range(func(key, value interface{}) bool {
		task := value.(*RegisteredTask)
		tasks = append(tasks, *task)
		return true
	})
	return tasks
}

// ExecuteTasksForAllTenants запускает все зарегистрированные задачи для всех активных тенантов
func (w *WorkerManager) ExecuteTasksForAllTenants(ctx context.Context) error {
	var errs []error

	w.registeredTasks.Range(func(key, value interface{}) bool {
		task := value.(*RegisteredTask)
		if err := w.executeTaskForAllTenants(ctx, task); err != nil {
			errs = append(errs, fmt.Errorf("failed to execute task %s: %w", task.Name, err))
		}
		return true
	})

	if len(errs) > 0 {
		return fmt.Errorf("errors executing tasks: %v", errs)
	}

	return nil
}

// ExecuteTaskForAllTenants запускает конкретную задачу для всех активных тенантов
func (w *WorkerManager) ExecuteTaskForAllTenants(ctx context.Context, taskID uuid.UUID) error {
	value, ok := w.registeredTasks.Load(taskID)
	if !ok {
		return fmt.Errorf("task with ID %s not found", taskID)
	}

	task := value.(*RegisteredTask)
	return w.executeTaskForAllTenants(ctx, task)
}

// ExecuteTaskForTenant запускает задачу для конкретного тенанта
func (w *WorkerManager) ExecuteTaskForTenant(
	ctx context.Context,
	taskID uuid.UUID,
	tenantID uuid.UUID,
) error {
	value, ok := w.registeredTasks.Load(taskID)
	if !ok {
		return fmt.Errorf("task with ID %s not found", taskID)
	}

	task := value.(*RegisteredTask)

	// Проверяем, активен ли тенант
	if _, ok := w.activeTenants.Load(tenantID); !ok {
		return fmt.Errorf("tenant %s is not active", tenantID)
	}

	return w.submitTask(ctx, task, tenantID)
}

// executeTaskForAllTenants отправляет задачу для каждого активного тенанта
func (w *WorkerManager) executeTaskForAllTenants(ctx context.Context, task *RegisteredTask) error {
	var errs []error
	tenantCount := 0

	w.activeTenants.Range(func(key, value interface{}) bool {
		tenantID := key.(uuid.UUID)
		if err := w.submitTask(ctx, task, tenantID); err != nil {
			// Логируем ошибку, но продолжаем для других тенантов
			w.logger.Warn("failed to submit task for tenant",
				slog.String("task_name", task.Name),
				slog.String("tenant_id", tenantID.String()),
				slog.Any("error", err))
			errs = append(errs, err)
		} else {
			tenantCount++
		}
		return true
	})

	w.logger.Debug("task submitted to tenants",
		slog.String("task_name", task.Name),
		slog.Int("tenant_count", tenantCount),
		slog.Int("error_count", len(errs)))

	if len(errs) > 0 && tenantCount == 0 {
		return fmt.Errorf("failed to submit task to any tenant: %v", errs)
	}

	return nil
}

// submitTask создаёт и отправляет задачу в пул
func (w *WorkerManager) submitTask(
	ctx context.Context,
	task *RegisteredTask,
	tenantID uuid.UUID,
) error {
	// Проверяем, не выполняется ли уже эта задача для этого тенанта
	taskKey := fmt.Sprintf("%s:%s", tenantID.String(), task.ID.String())
	if _, loaded := w.runningTenantTasks.LoadOrStore(taskKey, struct{}{}); loaded {
		return fmt.Errorf("task %s is already running for tenant %s", task.Name, tenantID)
	}

	// Определяем таймаут
	timeout := w.config.TaskTimeout
	if task.TaskTimeout != nil {
		timeout = *task.TaskTimeout
	}

	taskCtx, cancel := context.WithTimeout(ctx, timeout)

	// Создаём задачу
	poolTask := &Task{
		Ctx:      taskCtx,
		TaskID:   uuid.New(),
		TenantID: tenantID,
		Executor: &tenantTaskExecutor{
			executor:    task.Executor,
			taskContext: taskCtx,
		},
		Priority: task.Priority,
		Complete: func() {
			cancel()
			// Удаляем отметку о выполнении
			w.runningTenantTasks.Delete(taskKey)
		},
	}

	return w.pool.addTask(poolTask)
}

func (w *WorkerManager) refreshLoop() {
	defer w.wg.Done()

	// Интервал обновления тенантов
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

	// Обновляем карту активных тенантов
	newActive := make(map[uuid.UUID]Tenant, len(active))
	for _, tenant := range active {
		id := tenant.GetID()
		if id == uuid.Nil {
			continue
		}
		newActive[id] = tenant
		w.activeTenants.Store(id, tenant)
	}

	// Удаляем неактивные тенанты
	w.activeTenants.Range(func(key, value interface{}) bool {
		id := key.(uuid.UUID)
		if _, ok := newActive[id]; !ok {
			w.activeTenants.Delete(id)
			// Очищаем записи о выполняемых задачах для удалённого тенанта
			w.cleanupTenantTasks(id)
		}
		return true
	})

	SetActiveTenants(int64(len(newActive)))
	return nil
}

// cleanupTenantTasks очищает записи о выполняемых задачах для удалённого тенанта
func (w *WorkerManager) cleanupTenantTasks(tenantID uuid.UUID) {
	prefix := tenantID.String() + ":"
	w.runningTenantTasks.Range(func(key, value interface{}) bool {
		if k, ok := key.(string); ok && len(k) > len(prefix) && k[:len(prefix)] == prefix {
			w.runningTenantTasks.Delete(key)
		}
		return true
	})
}

func (w *WorkerManager) getActiveTenantCount() int {
	count := 0
	w.activeTenants.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// GetPool возвращает пул для использования в cron-менеджере
func (w *WorkerManager) GetPool() *pool {
	return w.pool
}
