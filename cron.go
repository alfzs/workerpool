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

type CronSchedule struct {
	ID       uuid.UUID
	CronExpr string
	TaskID   uuid.UUID // Ссылка на зарегистрированную задачу
	Config   CronTaskConfig
}

type CronTaskConfig struct {
	OverrideTimeout *time.Duration
	AllowOverlap    bool
}

type cronJob struct {
	schedule CronSchedule
	manager  *cronManager
	mu       sync.Mutex
}

type cronManager struct {
	cron      *cron.Cron
	schedules sync.Map
	entryIDs  sync.Map
	workerMgr *WorkerManager
	config    Config
	logger    *slog.Logger
}

func NewCronManager(workerMgr *WorkerManager, config Config, logger *slog.Logger) *cronManager {
	return &cronManager{
		cron:      cron.New(cron.WithSeconds()),
		workerMgr: workerMgr,
		config:    config,
		logger:    logger,
	}
}

func (cm *cronManager) Start() {
	cm.logger.Info("starting cron manager")
	cm.cron.Start()
}

func (cm *cronManager) Stop() {
	cm.logger.Info("stopping cron manager")
	cm.cron.Stop()
}

func (cm *cronManager) RegisterSchedule(schedule CronSchedule) error {
	if schedule.ID == uuid.Nil {
		schedule.ID = uuid.New()
	}

	// Проверяем, что задача зарегистрирована
	if _, err := cm.workerMgr.GetTask(schedule.TaskID); err != nil {
		return fmt.Errorf("task not registered: %w", err)
	}

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
		"task_id", schedule.TaskID.String(),
		"cron_expr", schedule.CronExpr)

	return nil
}

func (cm *cronManager) UnregisterSchedule(scheduleID uuid.UUID) error {
	entryIDValue, exists := cm.entryIDs.Load(scheduleID)
	if !exists {
		return fmt.Errorf("schedule with ID %s not found", scheduleID)
	}

	entryID := entryIDValue.(cron.EntryID)
	cm.cron.Remove(entryID)
	cm.schedules.Delete(scheduleID)
	cm.entryIDs.Delete(scheduleID)

	cm.logger.Info("unregistered cron schedule", "schedule_id", scheduleID.String())
	return nil
}

func (j *cronJob) Run() {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Получаем зарегистрированную задачу
	task, err := j.manager.workerMgr.GetTask(j.schedule.TaskID)
	if err != nil {
		j.manager.logger.Error("task not found for cron schedule",
			"schedule_id", j.schedule.ID.String(),
			"task_id", j.schedule.TaskID.String(),
			"error", err)
		return
	}

	timeout := j.manager.config.CronTaskTimeout
	if j.schedule.Config.OverrideTimeout != nil {
		timeout = *j.schedule.Config.OverrideTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Запускаем задачу для всех активных тенантов
	if err := j.manager.workerMgr.executeTaskForAllTenants(ctx, task); err != nil {
		j.manager.logger.Warn("cron task execution had errors",
			"schedule_id", j.schedule.ID.String(),
			"task_name", task.GetName(),
			"error", err)
	}

	j.manager.logger.Debug("cron job executed",
		"schedule_id", j.schedule.ID.String(),
		"task_name", task.GetName())
}
