package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/alfzs/backoff"
	"github.com/google/uuid"
)

// TaskDefinition represents a registered task that executes on a schedule.
// Each task has a unique identifier and execution interval.
type TaskDefinition struct {
	// ID uniquely identifies this task
	ID uuid.UUID

	// Name is a human-readable name for logging
	Name string

	// Interval specifies how often the task should execute
	Interval time.Duration

	// Executor is the function that performs the actual work
	Executor taskExecutor

	// JitterEnabled adds random delay before first execution
	// to distribute load across tasks
	JitterEnabled bool
}

// TaskRegistry manages registered tasks and their schedules.
// It works alongside WorkerManager to automatically execute tasks
// for all active tenants at configured intervals.
type TaskRegistry struct {
	manager *WorkerManager
	logger  *slog.Logger
	config  Config

	mu      sync.RWMutex
	tasks   map[uuid.UUID]*TaskDefinition
	cancels map[uuid.UUID]context.CancelFunc // for stopping schedulers
}

// NewTaskRegistry creates a new task registry associated with a WorkerManager.
func NewTaskRegistry(manager *WorkerManager) *TaskRegistry {
	return &TaskRegistry{
		manager: manager,
		logger:  manager.logger.With(slog.String("component", "task_registry")),
		config:  manager.config,
		tasks:   make(map[uuid.UUID]*TaskDefinition),
		cancels: make(map[uuid.UUID]context.CancelFunc),
	}
}

// RegisterTask registers a new task for periodic execution.
// The task will automatically execute for all active tenants at the specified interval.
//
// Parameters:
//   - taskID: unique identifier for the task (use uuid.New())
//   - name: human-readable name for logging
//   - interval: how often to execute the task
//   - executor: function that performs the actual work
//   - enableJitter: add random delay before first execution to distribute load
//
// Example:
//
//	taskID := uuid.New()
//	err := registry.RegisterTask(
//	    taskID,
//	    "sync_orders",
//	    5*time.Minute,
//	    &SyncOrdersExecutor{},
//	    true,
//	)
func (r *TaskRegistry) RegisterTask(
	taskID uuid.UUID,
	name string,
	interval time.Duration,
	executor taskExecutor,
	enableJitter bool,
) error {
	if taskID == uuid.Nil {
		return fmt.Errorf("task ID cannot be nil")
	}
	if interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}
	if executor == nil {
		return fmt.Errorf("executor cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if task already registered
	if _, exists := r.tasks[taskID]; exists {
		return fmt.Errorf("task with ID %s already registered", taskID)
	}

	task := &TaskDefinition{
		ID:            taskID,
		Name:          name,
		Interval:      interval,
		Executor:      executor,
		JitterEnabled: enableJitter,
	}

	r.tasks[taskID] = task

	// Start scheduler for this task
	r.startScheduler(task)

	r.logger.Info("task registered",
		slog.String("task_id", taskID.String()),
		slog.String("name", name),
		slog.Duration("interval", interval))

	return nil
}

// UnregisterTask removes a task from the registry and stops its scheduler.
func (r *TaskRegistry) UnregisterTask(taskID uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	task, exists := r.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Stop the scheduler
	if cancel, exists := r.cancels[taskID]; exists {
		cancel()
		delete(r.cancels, taskID)
	}

	delete(r.tasks, taskID)

	r.logger.Info("task unregistered",
		slog.String("task_id", taskID.String()),
		slog.String("name", task.Name))

	return nil
}

// startScheduler launches a goroutine that periodically executes the task
// for all active tenants.
func (r *TaskRegistry) startScheduler(task *TaskDefinition) {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancels[task.ID] = cancel

	go func() {
		defer func() {
			if rcv := recover(); rcv != nil {
				r.logger.Error("scheduler panic",
					slog.String("task_id", task.ID.String()),
					slog.String("name", task.Name),
					slog.Any("panic", rcv))
			}
		}()

		// Add jitter before first execution if enabled
		if task.JitterEnabled {
			jitter := r.calculateJitter()
			r.logger.Debug("adding jitter before first run",
				slog.String("task_id", task.ID.String()),
				slog.Duration("jitter", jitter))

			select {
			case <-time.After(jitter):
			case <-ctx.Done():
				return
			}
		}

		// Create ticker for periodic execution
		ticker := time.NewTicker(task.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.executeForAllTenants(task)
			}
		}
	}()
}

// executeForAllTenants executes the task for all currently active tenants.
func (r *TaskRegistry) executeForAllTenants(task *TaskDefinition) {
	tenants := r.manager.GetActiveTenants()

	if len(tenants) == 0 {
		r.logger.Debug("no active tenants, skipping task execution",
			slog.String("task_id", task.ID.String()),
			slog.String("name", task.Name))
		return
	}

	for _, tenantID := range tenants {
		// Create task with timeout
		ctx, cancel := context.WithTimeout(context.Background(), r.config.TaskTimeout)

		taskInstance := Task{
			Ctx:      ctx,
			TaskID:   task.ID,
			TenantID: tenantID,
			Executor: task.Executor,
			Complete: cancel,
		}

		// Submit to tenant's queue
		if err := r.manager.SubmitTask(tenantID, taskInstance); err != nil {
			cancel() // Clean up if submission fails
			r.logger.Warn("failed to submit scheduled task",
				slog.String("task_id", task.ID.String()),
				slog.String("name", task.Name),
				slog.String("tenant_id", tenantID.String()),
				slog.Any("error", err))
		}
	}
}

// calculateJitter computes a random delay between MinDelay and MaxDelay
// to distribute task execution across the cluster.
func (r *TaskRegistry) calculateJitter() time.Duration {
	return backoff.CalculateExponentialBackoff(
		1,
		r.config.RetryPolicy.Jitter.MinDelay,
		r.config.RetryPolicy.Jitter.MaxDelay,
	)
}
