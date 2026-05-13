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

type WorkerManager struct {
	logger   *slog.Logger
	provider tenantProvider
	config   Config
	pool     *pool

	ctx    context.Context
	cancel context.CancelFunc

	// Зарегистрированные задачи
	registeredTasks sync.Map // map[uuid.UUID]Task

	// Активные тенанты
	activeTenants sync.Map // map[uuid.UUID]Tenant

	// Для отслеживания выполняемых задач: ключ "tenantID:taskID"
	runningTenantTasks sync.Map // map[string]struct{}

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

	if err := w.refreshTenants(); err != nil {
		w.pool.stop()
		return fmt.Errorf("initial tenant refresh failed: %w", err)
	}

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

// RegisterTask регистрирует задачу в системе
func (w *WorkerManager) RegisterTask(task Task) error {
	if task == nil {
		return fmt.Errorf("task is nil")
	}

	taskID := task.GetID()
	if taskID == uuid.Nil {
		return fmt.Errorf("task ID is nil for task %s", task.GetName())
	}

	if _, exists := w.registeredTasks.Load(taskID); exists {
		return fmt.Errorf("task with ID %s (%s) already registered", taskID, task.GetName())
	}

	w.registeredTasks.Store(taskID, task)

	w.logger.Info("task registered",
		slog.String("task_id", taskID.String()),
		slog.String("task_name", task.GetName()),
		slog.Int("priority", int(task.GetPriority())))

	return nil
}

// UnregisterTask удаляет задачу из системы
func (w *WorkerManager) UnregisterTask(taskID uuid.UUID) error {
	value, exists := w.registeredTasks.Load(taskID)
	if !exists {
		return fmt.Errorf("task with ID %s not found", taskID)
	}

	task := value.(Task)
	w.registeredTasks.Delete(taskID)

	w.logger.Info("task unregistered",
		slog.String("task_id", taskID.String()),
		slog.String("task_name", task.GetName()))

	return nil
}

// GetTask возвращает зарегистрированную задачу по ID
func (w *WorkerManager) GetTask(taskID uuid.UUID) (Task, error) {
	value, ok := w.registeredTasks.Load(taskID)
	if !ok {
		return nil, fmt.Errorf("task with ID %s not found", taskID)
	}
	return value.(Task), nil
}

// ExecuteAllTasks запускает все зарегистрированные задачи для всех активных тенантов
func (w *WorkerManager) ExecuteAllTasks(ctx context.Context) error {
	var errs []error

	w.registeredTasks.Range(func(key, value interface{}) bool {
		task := value.(Task)
		if err := w.ExecuteTask(ctx, task.GetID()); err != nil {
			errs = append(errs, fmt.Errorf("task %s: %w", task.GetName(), err))
		}
		return true
	})

	if len(errs) > 0 {
		return fmt.Errorf("errors executing tasks: %v", errs)
	}

	return nil
}

// ExecuteTask запускает конкретную задачу для всех активных тенантов
func (w *WorkerManager) ExecuteTask(ctx context.Context, taskID uuid.UUID) error {
	task, err := w.GetTask(taskID)
	if err != nil {
		return err
	}

	return w.executeTaskForAllTenants(ctx, task)
}

// ExecuteTaskForTenant запускает задачу для конкретного тенанта
func (w *WorkerManager) ExecuteTaskForTenant(
	ctx context.Context,
	taskID uuid.UUID,
	tenantID uuid.UUID,
) error {
	task, err := w.GetTask(taskID)
	if err != nil {
		return err
	}

	if _, ok := w.activeTenants.Load(tenantID); !ok {
		return fmt.Errorf("tenant %s is not active", tenantID)
	}

	return w.submitTask(ctx, task, tenantID)
}

// executeTaskForAllTenants отправляет задачу для каждого активного тенанта
func (w *WorkerManager) executeTaskForAllTenants(ctx context.Context, task Task) error {
	var errs []error
	tenantCount := 0

	w.activeTenants.Range(func(key, value interface{}) bool {
		tenantID := key.(uuid.UUID)
		if err := w.submitTask(ctx, task, tenantID); err != nil {
			w.logger.Warn("failed to submit task for tenant",
				slog.String("task_name", task.GetName()),
				slog.String("tenant_id", tenantID.String()),
				slog.Any("error", err))
			errs = append(errs, err)
		} else {
			tenantCount++
		}
		return true
	})

	w.logger.Debug("task executed for tenants",
		slog.String("task_name", task.GetName()),
		slog.Int("tenant_count", tenantCount),
		slog.Int("error_count", len(errs)))

	if len(errs) > 0 && tenantCount == 0 {
		return fmt.Errorf("failed to execute task for any tenant: %v", errs)
	}

	return nil
}

// submitTask создаёт и отправляет задачу в пул
func (w *WorkerManager) submitTask(
	ctx context.Context,
	task Task,
	tenantID uuid.UUID,
) error {
	taskID := task.GetID()

	// Проверяем, не выполняется ли уже эта задача для этого тенанта
	taskKey := fmt.Sprintf("%s:%s", tenantID.String(), taskID.String())
	if _, loaded := w.runningTenantTasks.LoadOrStore(taskKey, struct{}{}); loaded {
		return fmt.Errorf("task %s is already running for tenant %s", task.GetName(), tenantID)
	}

	// Определяем таймаут
	timeout := w.config.TaskTimeout
	if taskTimeout := task.GetTimeout(); taskTimeout != nil {
		timeout = *taskTimeout
	}

	taskCtx, cancel := context.WithTimeout(ctx, timeout)

	// Создаём задачу для пула
	poolTask := &PoolTask{
		Ctx:      taskCtx,
		TaskID:   uuid.New(), // Уникальный ID для этого запуска
		TenantID: tenantID,
		Executor: &taskAdapter{
			task:        task,
			taskContext: taskCtx,
		},
		Priority: task.GetPriority(),
		Complete: func() {
			cancel()
			w.runningTenantTasks.Delete(taskKey)
		},
	}

	return w.pool.addTask(poolTask)
}

// taskAdapter адаптирует интерфейс Task для использования в пуле
type taskAdapter struct {
	task        Task
	taskContext context.Context
}

func (a *taskAdapter) Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error {
	// Используем сохранённый контекст с таймаутом
	return a.task.Execute(a.taskContext, tenantID, workerID)
}

func (w *WorkerManager) refreshLoop() {
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

func (w *WorkerManager) refreshTenants() error {
	active, err := w.provider.GetActive(w.ctx)
	if err != nil {
		return fmt.Errorf("get active tenants: %w", err)
	}

	newActive := make(map[uuid.UUID]Tenant, len(active))
	for _, tenant := range active {
		id := tenant.GetID()
		if id == uuid.Nil {
			continue
		}
		newActive[id] = tenant
		w.activeTenants.Store(id, tenant)
	}

	w.activeTenants.Range(func(key, value interface{}) bool {
		id := key.(uuid.UUID)
		if _, ok := newActive[id]; !ok {
			w.activeTenants.Delete(id)
			w.cleanupTenantTasks(id)
		}
		return true
	})

	SetActiveTenants(int64(len(newActive)))
	return nil
}

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

// GetActiveTenants возвращает список активных тенантов
func (w *WorkerManager) GetActiveTenants() []uuid.UUID {
	var tenants []uuid.UUID
	w.activeTenants.Range(func(key, value interface{}) bool {
		tenants = append(tenants, key.(uuid.UUID))
		return true
	})
	return tenants
}
