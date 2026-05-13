package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// CronSchedule представляет зарегистрированную cron-задачу для тенанта.
type CronSchedule struct {
	ID       uuid.UUID
	TenantID uuid.UUID
	CronExpr string
	Executor taskExecutor
	Config   CronTaskConfig
}

// CronTaskConfig содержит настройки для cron-задачи.
type CronTaskConfig struct {
	// OverrideTimeout позволяет переопределить таймаут из глобального конфига
	OverrideTimeout *time.Duration

	// Priority задаёт приоритет cron-задачи (по умолчанию PriorityNormal)
	Priority TaskPriority

	// AllowOverlap позволяет запускать задачу, даже если предыдущая еще не завершена
	AllowOverlap bool
}

// cronJob представляет обертку для cron-задачи.
type cronJob struct {
	schedule CronSchedule
	manager  *cronManager
	mu       sync.Mutex
}

// cronManager управляет всеми cron-задачами.
type cronManager struct {
	cron      *cron.Cron
	schedules sync.Map // map[uuid.UUID]*cronJob
	entryIDs  sync.Map // map[uuid.UUID]cron.EntryID
	workerMgr *WorkerManager
	config    Config
	logger    *slog.Logger
}

// NewCronManager создает новый менеджер cron-задач.
func NewCronManager(workerMgr *WorkerManager, config Config, logger *slog.Logger) *cronManager {
	return &cronManager{
		cron:      cron.New(cron.WithSeconds()),
		workerMgr: workerMgr,
		config:    config,
		logger:    logger,
	}
}

// Start запускает cron-менеджер.
func (cm *cronManager) Start() {
	cm.logger.Info("starting cron manager")
	cm.cron.Start()
}

// Stop останавливает cron-менеджер.
func (cm *cronManager) Stop() {
	cm.logger.Info("stopping cron manager")
	// Останавливаем все запланированные задачи
	cm.schedules.Range(func(key, value interface{}) bool {
		scheduleID := key.(uuid.UUID)
		if entryIDVal, ok := cm.entryIDs.Load(scheduleID); ok {
			entryID := entryIDVal.(cron.EntryID)
			cm.cron.Remove(entryID)
		}
		return true
	})
	cm.cron.Stop()
}

// RegisterSchedule регистрирует новую cron-задачу для тенанта.
func (cm *cronManager) RegisterSchedule(schedule CronSchedule) error {
	if schedule.TenantID == uuid.Nil {
		return fmt.Errorf("tenant ID is required")
	}

	if schedule.ID == uuid.Nil {
		schedule.ID = uuid.New()
	}

	// Устанавливаем приоритет по умолчанию, если не задан
	if schedule.Config.Priority == 0 {
		schedule.Config.Priority = PriorityNormal
	}

	// Проверяем, не зарегистрирована ли уже задача с таким ID
	if _, exists := cm.schedules.Load(schedule.ID); exists {
		return fmt.Errorf("schedule with ID %s already exists", schedule.ID)
	}

	job := &cronJob{
		schedule: schedule,
		manager:  cm,
	}

	entryID, err := cm.cron.AddJob(schedule.CronExpr, job)
	if err != nil {
		return fmt.Errorf("failed to add cron job: %w", err)
	}

	cm.schedules.Store(schedule.ID, job)
	cm.entryIDs.Store(schedule.ID, entryID)

	cm.logger.Info("registered cron schedule",
		"schedule_id", schedule.ID.String(),
		"tenant_id", schedule.TenantID.String(),
		"cron_expr", schedule.CronExpr,
		"priority", schedule.Config.Priority)

	return nil
}

// UnregisterSchedule удаляет cron-задачу.
func (cm *cronManager) UnregisterSchedule(scheduleID uuid.UUID) error {
	entryIDValue, exists := cm.entryIDs.Load(scheduleID)
	if !exists {
		return fmt.Errorf("schedule with ID %s not found", scheduleID)
	}

	entryID := entryIDValue.(cron.EntryID)
	cm.cron.Remove(entryID)
	cm.schedules.Delete(scheduleID)
	cm.entryIDs.Delete(scheduleID)

	cm.logger.Info("unregistered cron schedule",
		"schedule_id", scheduleID.String())

	return nil
}

// GetSchedule возвращает зарегистрированную cron-задачу.
func (cm *cronManager) GetSchedule(scheduleID uuid.UUID) (*CronSchedule, error) {
	value, exists := cm.schedules.Load(scheduleID)
	if !exists {
		return nil, fmt.Errorf("schedule with ID %s not found", scheduleID)
	}

	job := value.(*cronJob)
	return &job.schedule, nil
}

// GetTenantSchedules возвращает все cron-задачи для указанного тенанта.
func (cm *cronManager) GetTenantSchedules(tenantID uuid.UUID) []CronSchedule {
	var schedules []CronSchedule

	cm.schedules.Range(func(key, value interface{}) bool {
		job := value.(*cronJob)
		if job.schedule.TenantID == tenantID {
			schedules = append(schedules, job.schedule)
		}
		return true
	})

	return schedules
}

// Run implements cron.Job interface.
func (j *cronJob) Run() {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Проверяем, разрешено ли перекрытие задач
	if !j.schedule.Config.AllowOverlap {
		// Пул сам проверит уникальность задачи по TaskID,
		// поэтому AllowOverlap теперь управляется через разные TaskID
	}

	// Определяем таймаут
	timeout := j.manager.config.CronTaskTimeout
	if j.schedule.Config.OverrideTimeout != nil {
		timeout = *j.schedule.Config.OverrideTimeout
	}

	// Создаем контекст с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Создаем уникальный ID задачи для проверки на дублирование
	taskID := uuid.New()

	// Если AllowOverlap разрешён, каждая задача будет иметь уникальный ID,
	// что позволит выполняться параллельно
	if j.schedule.Config.AllowOverlap {
		taskID = uuid.New()
	} else {
		// Если перекрытие запрещено, используем ID расписания как основу,
		// чтобы пул мог отклонить дубликат
		taskID = j.schedule.ID
	}

	taskCtx, taskCancel := context.WithTimeout(ctx, timeout)

	// Оборачиваем оригинальный executor
	wrappedExecutor := &cronTaskExecutor{
		executor:    j.schedule.Executor,
		taskContext: taskCtx,
	}

	// Создаем задачу напрямую для пула
	task := &Task{
		Ctx:      taskCtx,
		TaskID:   taskID,
		TenantID: j.schedule.TenantID,
		Executor: wrappedExecutor,
		Priority: j.schedule.Config.Priority,
		Complete: taskCancel,
	}

	// Отправляем задачу напрямую в пул
	if err := j.manager.workerMgr.pool.addTask(task); err != nil {
		j.manager.logger.Warn("failed to submit cron task to pool",
			"schedule_id", j.schedule.ID.String(),
			"tenant_id", j.schedule.TenantID.String(),
			"error", err)
		return
	}

	j.manager.logger.Debug("triggered cron job",
		"schedule_id", j.schedule.ID.String(),
		"tenant_id", j.schedule.TenantID.String(),
		"task_id", taskID.String(),
		"priority", j.schedule.Config.Priority,
		"timeout", timeout)
}

// cronTaskExecutor адаптирует контекст для cron-задачи
type cronTaskExecutor struct {
	executor    taskExecutor
	taskContext context.Context
}

func (e *cronTaskExecutor) Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error {
	// Игнорируем переданный ctx, используем сохранённый контекст с правильным таймаутом
	return e.executor.Execute(e.taskContext, tenantID, workerID)
}
