package workerpool

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- Базовый старт/стоп ---

func TestManagerStartStop(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 2}})

	m, err := NewWorkerManager(WorkerManagerParams{
		TenantProvider: provider,
		Config:         newTestConfig(),
	})
	if err != nil {
		t.Fatalf("NewWorkerManager: %v", err)
	}

	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	h := m.Health()
	if !h.Healthy {
		t.Error("expected Healthy=true after Start")
	}

	m.Stop()

	h = m.Health()
	if h.Healthy {
		t.Error("expected Healthy=false after Stop")
	}

	if !h.Stopping {
		t.Error("expected Stopping=true after Stop")
	}
}

func TestManagerNewWorkerManager_InvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := NewWorkerManager(WorkerManagerParams{
		TenantProvider: &mockTenantProvider{},
		Config:         Config{}, // нулевые значения — невалидно
	})
	if err == nil {
		t.Error("expected error for invalid config, got nil")
	}
}

// --- SubmitTask ---

func TestSubmitToUnknownTenant(t *testing.T) {
	t.Parallel()

	provider := &mockTenantProvider{} // пустой провайдер
	m := startManager(t, provider, newTestConfig())

	err := m.SubmitTask(uuid.New(), newTask(uuid.New(), successExec(), func(error) {}))
	if err == nil {
		t.Error("expected error for unknown tenant, got nil")
	}
}

func TestSubmitTaskCompletesSuccessfully(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})
	m := startManager(t, provider, newTestConfig())

	done := make(chan error, 1)

	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(err error) { done <- err })); err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("task error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task did not complete")
	}
}

// --- Валидация Task в SubmitTask ---

func TestSubmitTaskNilContext(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})
	m := startManager(t, provider, newTestConfig())

	task := newTask(tenantID, successExec(), func(error) {})
	task.Ctx = nil

	if err := m.SubmitTask(tenantID, task); !errors.Is(err, ErrTaskNilContext) {
		t.Errorf("expected ErrTaskNilContext, got %v", err)
	}
}

func TestSubmitTaskNoExecutor(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})
	m := startManager(t, provider, newTestConfig())

	task := newTask(tenantID, nil, func(error) {})

	if err := m.SubmitTask(tenantID, task); !errors.Is(err, ErrTaskNoExecutor) {
		t.Errorf("expected ErrTaskNoExecutor, got %v", err)
	}
}

func TestSubmitTaskExecutorKeyWithoutRegistry(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})
	m := startManager(t, provider, newTestConfig())

	task := newTask(tenantID, nil, func(error) {})
	task.ExecutorKey = "sync_orders"

	if err := m.SubmitTask(tenantID, task); !errors.Is(err, ErrNoExecutorRegistry) {
		t.Errorf("expected ErrNoExecutorRegistry, got %v", err)
	}
}

func TestSubmitTaskExecutorKeyUnknown(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	m, err := NewWorkerManager(WorkerManagerParams{
		TenantProvider:   provider,
		Config:           newTestConfig(),
		ExecutorRegistry: NewExecutorRegistry(),
	})
	if err != nil {
		t.Fatalf("NewWorkerManager: %v", err)
	}

	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(m.Stop)

	task := newTask(tenantID, nil, func(error) {})
	task.ExecutorKey = "missing"

	if err := m.SubmitTask(tenantID, task); !errors.Is(err, ErrExecutorNotFound) {
		t.Errorf("expected ErrExecutorNotFound, got %v", err)
	}
}

func TestSubmitTaskExecutorKeyResolved(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	registry := NewExecutorRegistry()
	registry.MustRegister("sync_orders", successExec())

	m, err := NewWorkerManager(WorkerManagerParams{
		TenantProvider:   provider,
		Config:           newTestConfig(),
		ExecutorRegistry: registry,
	})
	if err != nil {
		t.Fatalf("NewWorkerManager: %v", err)
	}

	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(m.Stop)

	done := make(chan error, 1)
	task := newTask(tenantID, nil, func(err error) { done <- err })
	task.ExecutorKey = "sync_orders"

	if err := m.SubmitTask(tenantID, task); err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("task error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task did not complete")
	}
}

// --- Изоляция тенантов ---

// TestTenantConcurrencyLimit проверяет, что семафор не даёт запустить
// одновременно больше WorkerLimit задач для одного тенанта.
func TestTenantConcurrencyLimit(t *testing.T) {
	t.Parallel()

	const limit = 3

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: limit}})

	cfg := newTestConfig()
	cfg.WorkerCount = 32
	m := startManager(t, provider, cfg)

	var running atomic.Int32

	atLimit := make(chan struct{}, 1)
	release := make(chan struct{})

	var wg sync.WaitGroup

	exec := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		if int(running.Add(1)) == limit {
			select {
			case atLimit <- struct{}{}:
			default:
			}
		}

		<-release
		running.Add(-1)

		return nil
	}}

	// Отправляем задач больше лимита.
	const total = limit + 2
	wg.Add(total)

	for range total {
		if err := m.SubmitTask(tenantID, newTask(tenantID, exec, func(error) { wg.Done() })); err != nil {
			t.Fatalf("SubmitTask: %v", err)
		}
	}

	// Ждём сигнала, что ровно `limit` задач запущено одновременно.
	select {
	case <-atLimit:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for tasks to reach the concurrency limit")
	}

	// Семафор заполнен: (limit+1)-я задача должна ждать в dispatcher'е.
	if n := int(running.Load()); n != limit {
		t.Errorf("concurrency limit broken: expected %d running, got %d", limit, n)
	}

	close(release)
	wg.Wait()
}

// TestTenantIsolation проверяет, что занятый семафор тенанта A
// не блокирует выполнение задач тенанта B.
func TestTenantIsolation(t *testing.T) {
	t.Parallel()

	tenantA := uuid.New()
	tenantB := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{
		&mockTenant{id: tenantA, limit: 1},
		&mockTenant{id: tenantB, limit: 1},
	})

	cfg := newTestConfig()
	cfg.WorkerCount = 16
	m := startManager(t, provider, cfg)

	// Блокируем тенанта A: его семафор занят.
	aRunning := make(chan struct{})
	releaseA := make(chan struct{})

	execA := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		close(aRunning)
		<-releaseA

		return nil
	}}
	if err := m.SubmitTask(tenantA, newTask(tenantA, execA, func(error) {})); err != nil {
		t.Fatalf("SubmitTask A: %v", err)
	}

	select {
	case <-aRunning:
	case <-time.After(5 * time.Second):
		t.Fatal("tenant A task did not start")
	}

	// Отправляем задачу тенанту B — должна выполниться без ожидания A.
	bDone := make(chan error, 1)

	if err := m.SubmitTask(tenantB, newTask(tenantB, successExec(), func(err error) { bDone <- err })); err != nil {
		t.Fatalf("SubmitTask B: %v", err)
	}

	select {
	case err := <-bDone:
		if err != nil {
			t.Errorf("tenant B task failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("tenant B was blocked by tenant A (isolation broken)")
	}

	close(releaseA)
}

// TestMultipleTenantsConcurrent проверяет, что множество тенантов
// работают одновременно без взаимной блокировки.
func TestMultipleTenantsConcurrent(t *testing.T) {
	t.Parallel()

	const tenantCount = 8

	tenants := make([]Tenant, tenantCount)

	ids := make([]uuid.UUID, tenantCount)
	for i := range tenantCount {
		ids[i] = uuid.New()
		tenants[i] = &mockTenant{id: ids[i], limit: 2}
	}

	provider := &mockTenantProvider{}
	provider.set(tenants)

	cfg := newTestConfig()
	cfg.WorkerCount = 64
	m := startManager(t, provider, cfg)

	var wg sync.WaitGroup

	const tasksPerTenant = 4
	wg.Add(tenantCount * tasksPerTenant)

	for _, id := range ids {
		for range tasksPerTenant {
			if err := m.SubmitTask(id, newTask(id, successExec(), func(error) { wg.Done() })); err != nil {
				t.Errorf("SubmitTask: %v", err)
			}
		}
	}

	done := make(chan struct{})

	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Error("tasks did not complete — possible cross-tenant deadlock")
	}
}

// --- Complete вызывается ровно один раз ---

// TestCompleteCalledExactlyOnce проверяет инвариант «Complete вызывается
// ровно один раз» для трёх путей: успех, ошибка, паника.
func TestCompleteCalledExactlyOnce(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 4}})
	m := startManager(t, provider, newTestConfig())

	scenarios := []struct {
		name string
		exec *mockExecutor
	}{
		{
			"success",
			&mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error { return nil }},
		},
		{
			"error",
			&mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
				return errors.New("intentional error")
			}},
		},
		{
			"panic",
			&mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
				panic("intentional panic")
			}},
		},
	}

	for _, sc := range scenarios { //nolint:paralleltest // subtests share one manager/tenant and would race on its queue capacity if parallelized
		t.Run(sc.name, func(t *testing.T) {
			var count atomic.Int32

			called := make(chan struct{})

			if err := m.SubmitTask(tenantID, newTask(tenantID, sc.exec, func(error) {
				if count.Add(1) == 1 {
					close(called)
				}
			})); err != nil {
				t.Fatalf("SubmitTask: %v", err)
			}

			select {
			case <-called:
			case <-time.After(5 * time.Second):
				t.Fatal("Complete was never called")
			}

			// Небольшая пауза, чтобы поймать гипотетический второй вызов.
			time.Sleep(50 * time.Millisecond)

			if n := count.Load(); n != 1 {
				t.Errorf("Complete called %d times, want exactly 1", n)
			}
		})
	}
}

// --- Восстановление после паники ---

// TestPanicDoesNotKillPool проверяет, что паника в Executor не убивает
// горутину-воркер: после паники пул продолжает обрабатывать задачи.
func TestPanicDoesNotKillPool(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 4}})
	m := startManager(t, provider, newTestConfig())

	panicExec := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		panic("test panic")
	}}
	panicDone := make(chan error, 1)

	if err := m.SubmitTask(tenantID, newTask(tenantID, panicExec, func(err error) { panicDone <- err })); err != nil {
		t.Fatalf("submit panicking task: %v", err)
	}

	select {
	case err := <-panicDone:
		if err == nil {
			t.Error("expected non-nil error from panicking task")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("panicking task did not call Complete")
	}

	// Пул должен продолжать работать после паники.
	normalDone := make(chan error, 1)

	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(err error) { normalDone <- err })); err != nil {
		t.Fatalf("submit normal task after panic: %v", err)
	}

	select {
	case err := <-normalDone:
		if err != nil {
			t.Errorf("normal task after panic failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not recover after worker panic")
	}
}

// --- Повторные попытки ---

// TestTaskRetryOnError проверяет, что при ошибке Executor вызывается
// повторно заданное число раз согласно RetryPolicy.
func TestTaskRetryOnError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	cfg := newTestConfig()
	cfg.RetryPolicy.Attempts.Count = 3
	cfg.RetryPolicy.Attempts.MinDelay = time.Millisecond
	cfg.RetryPolicy.Attempts.MaxDelay = 5 * time.Millisecond
	m := startManager(t, provider, cfg)

	var attempts atomic.Int32

	exec := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		attempts.Add(1)
		return errors.New("transient error")
	}}

	done := make(chan error, 1)

	if err := m.SubmitTask(tenantID, newTask(tenantID, exec, func(err error) { done <- err })); err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected error after exhausting retries")
		}

		if n := int(attempts.Load()); n != 3 {
			t.Errorf("expected 3 attempts, got %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task did not complete after retries")
	}
}

// --- Контекст ---

// TestExecuteRespectsConfiguredTaskTimeout проверяет, что Config.TaskTimeout
// реально ограничивает выполнение задачи, даже если Task.Ctx не несёт
// собственного дедлайна (docs/DESIGN_PATTERNS_AUDIT.md, находка №1).
func TestExecuteRespectsConfiguredTaskTimeout(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	cfg := newTestConfig()
	cfg.TaskTimeout = 50 * time.Millisecond
	cfg.RetryPolicy.Attempts.Count = 1
	m := startManager(t, provider, cfg)

	started := make(chan struct{})
	exec := &mockExecutor{fn: func(ctx context.Context, _ uuid.UUID, _ int) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	}}

	done := make(chan error, 1)

	// Task.Ctx намеренно без собственного дедлайна — ограничение должно
	// прийти исключительно из Config.TaskTimeout.
	if err := m.SubmitTask(tenantID, Task{
		Ctx:      context.Background(),
		TaskID:   uuid.New(),
		TenantID: tenantID,
		Executor: exec,
		Complete: func(err error) { done <- err },
	}); err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}

	<-started

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task did not time out — Config.TaskTimeout was not applied")
	}
}

// TestContextCancellationPropagated проверяет, что отмена Task.Ctx
// распространяется в Executor.Execute.
func TestContextCancellationPropagated(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})
	m := startManager(t, provider, newTestConfig())

	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	exec := &mockExecutor{fn: func(ctx context.Context, _ uuid.UUID, _ int) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	}}

	done := make(chan error, 1)

	if err := m.SubmitTask(tenantID, Task{
		Ctx:      ctx,
		TaskID:   uuid.New(),
		TenantID: tenantID,
		Executor: exec,
		Complete: func(err error) { done <- err },
	}); err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}

	<-started
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Error("expected non-nil error from cancelled task")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task did not react to context cancellation")
	}
}

// --- Очередь тенанта ---

// TestTenantQueueFull проверяет, что при заполненном буфере SubmitTask
// немедленно возвращает ошибку без блокировки.
func TestTenantQueueFull(t *testing.T) {
	t.Parallel()

	const queueSize = 2

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	cfg := newTestConfig()
	cfg.TenantQueueSize = queueSize
	cfg.WorkerCount = 16
	m := startManager(t, provider, cfg)

	// Задача 1: занимает семафор (limit=1). Диспетчер будет заблокирован
	// на sem.Acquire при попытке взять следующую задачу.
	running := make(chan struct{})
	release := make(chan struct{})

	blocker := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		close(running)
		<-release

		return nil
	}}
	if err := m.SubmitTask(tenantID, newTask(tenantID, blocker, func(error) {})); err != nil {
		t.Fatalf("task 1 submit: %v", err)
	}

	select {
	case <-running:
	case <-time.After(5 * time.Second):
		t.Fatal("task 1 did not start")
	}

	// Задача 2: диспетчер её подхватит и заблокируется на sem.Acquire.
	// Даём небольшую паузу, чтобы диспетчер успел перейти в это состояние.
	_ = m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(error) {}))

	time.Sleep(30 * time.Millisecond)

	// Заполняем taskQueue до ёмкости.
	for i := range queueSize {
		if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(error) {})); err != nil {
			t.Fatalf("fill slot %d: unexpected error: %v", i, err)
		}
	}

	// Следующая отправка должна вернуть ошибку «очередь заполнена».
	err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(error) {}))
	if err == nil {
		t.Error("expected queue-full error, got nil")
	}

	close(release)
}

// --- Обновление тенантов ---

// TestRefreshTenantsAddTenant проверяет, что после добавления тенанта
// через TenantProvider и вызова refreshTenants задачи для него принимаются.
func TestRefreshTenantsAddTenant(t *testing.T) {
	t.Parallel()

	provider := &mockTenantProvider{} // изначально пустой
	m := startManager(t, provider, newTestConfig())

	tenantID := uuid.New()
	// Тенант ещё не зарегистрирован — ожидаем ошибку.
	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(error) {})); err == nil {
		t.Fatal("expected error for unknown tenant before refresh")
	}

	// Добавляем тенанта и принудительно обновляем.
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	if err := m.refreshTenants(); err != nil {
		t.Fatalf("refreshTenants: %v", err)
	}

	done := make(chan error, 1)

	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(err error) { done <- err })); err != nil {
		t.Fatalf("SubmitTask after refresh: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("task after add: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task did not complete after tenant was added")
	}
}

// TestRefreshTenantsRemoveTenant проверяет, что после удаления тенанта
// из провайдера SubmitTask возвращает ошибку.
func TestRefreshTenantsRemoveTenant(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})
	m := startManager(t, provider, newTestConfig())

	// Убеждаемся, что тенант работает.
	done := make(chan error, 1)

	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(err error) { done <- err })); err != nil {
		t.Fatalf("initial submit: %v", err)
	}

	<-done

	// Удаляем тенанта и обновляем.
	provider.set(nil)

	if err := m.refreshTenants(); err != nil {
		t.Fatalf("refreshTenants: %v", err)
	}

	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(error) {})); err == nil {
		t.Error("expected error after tenant removed, got nil")
	}
}

// TestRefreshTenantsRemoveTenantDrainsQueue проверяет, что задачи, оставшиеся
// в буфере taskQueue тенанта на момент его удаления, всё равно получают
// Task.Complete(ErrDispatcherStopped) — а не теряются молча (SECURITY_AUDIT.md,
// находка №1).
func TestRefreshTenantsRemoveTenantDrainsQueue(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})
	m := startManager(t, provider, newTestConfig())

	blockCh := make(chan struct{})
	blocker := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		<-blockCh
		return nil
	}}

	// Задача A занимает единственный слот конкурентности тенанта — dispatch
	// блокируется в sem.Acquire для следующей задачи.
	aDone := make(chan error, 1)

	if err := m.SubmitTask(tenantID, newTask(tenantID, blocker, func(err error) { aDone <- err })); err != nil {
		t.Fatalf("submit A: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Задача B — на ней dispatch блокируется в sem.Acquire.
	bDone := make(chan error, 1)

	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(err error) { bDone <- err })); err != nil {
		t.Fatalf("submit B: %v", err)
	}

	// Задача C остаётся непрочитанной в буфере taskQueue.
	cDone := make(chan error, 1)

	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(err error) { cDone <- err })); err != nil {
		t.Fatalf("submit C: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Удаляем тенанта, пока A ещё выполняется, а B и C ждут в очереди.
	provider.set(nil)

	if err := m.refreshTenants(); err != nil {
		t.Fatalf("refreshTenants: %v", err)
	}

	close(blockCh)

	select {
	case <-aDone:
	case <-time.After(5 * time.Second):
		t.Error("A: Complete never called")
	}

	select {
	case err := <-bDone:
		if !errors.Is(err, ErrDispatcherStopped) {
			t.Errorf("B: expected ErrDispatcherStopped, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("B: Complete never called")
	}

	select {
	case err := <-cDone:
		if !errors.Is(err, ErrDispatcherStopped) {
			t.Errorf("C: expected ErrDispatcherStopped, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("C: Complete never called")
	}
}

// TestRefreshTenantsUpdateLimit проверяет, что увеличение WorkerLimit
// вступает в силу после refreshTenants: новое число задач выполняется
// одновременно.
func TestRefreshTenantsUpdateLimit(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	cfg := newTestConfig()
	cfg.WorkerCount = 16
	m := startManager(t, provider, cfg)

	const newLimit = 3
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: newLimit}})

	if err := m.refreshTenants(); err != nil {
		t.Fatalf("refreshTenants: %v", err)
	}

	// После увеличения лимита newLimit задач должны выполняться одновременно.
	var running atomic.Int32

	atLimit := make(chan struct{}, 1)
	release := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(newLimit)

	exec := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		if int(running.Add(1)) == newLimit {
			select {
			case atLimit <- struct{}{}:
			default:
			}
		}

		<-release
		running.Add(-1)

		return nil
	}}
	for range newLimit {
		if err := m.SubmitTask(tenantID, newTask(tenantID, exec, func(error) { wg.Done() })); err != nil {
			t.Fatalf("SubmitTask: %v", err)
		}
	}

	select {
	case <-atLimit:
	case <-time.After(5 * time.Second):
		t.Fatal("did not reach new concurrency limit after refresh")
	}

	if n := int(running.Load()); n != newLimit {
		t.Errorf("expected %d concurrent tasks after limit update, got %d", newLimit, n)
	}

	close(release)
	wg.Wait()
}

// TestSetWorkerCountNoGenerationOverlap проверяет, что задачи, отправленные
// сразу после увеличения WorkerLimit, не могут быть перехвачены диспетчером
// уходящего поколения и отклонены с ErrDispatcherStopped вместо выполнения
// под новым лимитом (docs/CONCURRENCY_AUDIT.md, находка №1).
func TestSetWorkerCountNoGenerationOverlap(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	cfg := newTestConfig()
	cfg.WorkerCount = 16
	m := startManager(t, provider, cfg)

	holdRelease := make(chan struct{})
	holdDone := make(chan error, 1)
	holder := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		<-holdRelease
		return nil
	}}

	if err := m.SubmitTask(tenantID, newTask(tenantID, holder, func(err error) { holdDone <- err })); err != nil {
		t.Fatalf("submit holder: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	const newLimit = 4

	provider.set([]Tenant{&mockTenant{id: tenantID, limit: newLimit}})

	if err := m.refreshTenants(); err != nil {
		t.Fatalf("refreshTenants: %v", err)
	}

	var running atomic.Int32

	atLimit := make(chan struct{}, 1)
	release := make(chan struct{})
	results := make(chan error, newLimit)

	exec := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		if int(running.Add(1)) == newLimit {
			select {
			case atLimit <- struct{}{}:
			default:
			}
		}

		<-release
		running.Add(-1)

		return nil
	}}

	for range newLimit {
		if err := m.SubmitTask(tenantID, newTask(tenantID, exec, func(err error) { results <- err })); err != nil {
			t.Fatalf("SubmitTask: %v", err)
		}
	}

	waitSignal(t, atLimit, 5*time.Second,
		"burst tasks did not reach new concurrency limit — a task may have been claimed by the outgoing generation")

	close(release)

	for range newLimit {
		if err := waitComplete(t, results, 5*time.Second, "burst task Complete never called"); err != nil {
			t.Errorf("burst task completed with unexpected error: %v", err)
		}
	}

	close(holdRelease)

	if err := waitComplete(t, holdDone, 5*time.Second, "holder task Complete never called"); err != nil {
		t.Errorf("holder task completed with unexpected error: %v", err)
	}
}

// TestSubmitTaskNoRaceWithTenantRemoval проверяет, что SubmitTask не может
// отправить задачу в taskQueue тенанта, конкурентно удаляемого refreshTenants:
// до фикса окно между RUnlock и select позволяло задаче попасть в уже
// осиротевший канал и потерять Complete навсегда (docs/CONCURRENCY_AUDIT.md,
// находка №2). Тест переключает присутствие тенанта в цикле, конкурентно
// с пачкой горутин, непрерывно вызывающих SubmitTask, и проверяет, что число
// принятых задач равно числу вызовов Complete.
func TestSubmitTaskNoRaceWithTenantRemoval(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 2}})

	cfg := newTestConfig()
	cfg.TenantQueueSize = 8
	cfg.TenantRefreshInterval = time.Hour

	m := startManager(t, provider, cfg)

	var submitted, completed atomic.Int64

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		present := true

		for {
			select {
			case <-stop:
				return
			default:
			}

			present = !present
			if present {
				provider.set([]Tenant{&mockTenant{id: tenantID, limit: 2}})
			} else {
				provider.set(nil)
			}

			_ = m.refreshTenants()
		}
	})

	for range 8 {
		wg.Go(func() {
			exec := successExec()
			for range 2000 {
				task := newTask(tenantID, exec, func(error) { completed.Add(1) })
				if err := m.SubmitTask(tenantID, task); err == nil {
					submitted.Add(1)
				}
			}
		})
	}

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && completed.Load() != submitted.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	if completed.Load() != submitted.Load() {
		t.Errorf("leaked tasks: submitted=%d completed=%d (gap=%d)",
			submitted.Load(), completed.Load(), submitted.Load()-completed.Load())
	}
}

// --- Graceful shutdown ---

// TestStopDoesNotDeadlock проверяет, что Stop() завершается в течение
// GracefulTimeout даже при наличии заблокированных задач в пуле.
func TestStopDoesNotDeadlock(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 4}})

	cfg := newTestConfig()
	cfg.GracefulTimeout = 300 * time.Millisecond
	cfg.WorkerCount = 4

	// Не используем startManager: Stop() вызовем вручную.
	m, err := NewWorkerManager(WorkerManagerParams{TenantProvider: provider, Config: cfg})
	if err != nil {
		t.Fatalf("NewWorkerManager: %v", err)
	}

	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Stop уже вызывается вручную ниже, но t.Cleanup — страховка на случай
	// t.Fatalf между Start и ручным Stop (Stop идемпотентен, см. TestStopIdempotent).
	t.Cleanup(m.Stop)

	// Запускаем задачи, которые блокируются на ctx.Done().
	var started sync.WaitGroup
	started.Add(4)

	exec := &mockExecutor{fn: func(ctx context.Context, _ uuid.UUID, _ int) error {
		started.Done()
		<-ctx.Done()

		return ctx.Err()
	}}
	for range 4 {
		if err := m.SubmitTask(tenantID, newTask(tenantID, exec, func(error) {})); err != nil {
			t.Fatalf("SubmitTask: %v", err)
		}
	}

	started.Wait()

	// Stop должен вернуться после forceCancel (≈ GracefulTimeout).
	stopDone := make(chan struct{})

	go func() { m.Stop(); close(stopDone) }()

	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Error("Stop() deadlocked or took too long")
	}
}

// TestStopIdempotent проверяет, что повторный вызов Stop() не вызывает
// паники и не блокирует горутину.
func TestStopIdempotent(t *testing.T) {
	t.Parallel()

	provider := &mockTenantProvider{}
	m := startManager(t, provider, newTestConfig())

	done := make(chan struct{})

	go func() {
		m.Stop()
		m.Stop() // повторный вызов — должен быть no-op
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("second Stop() call deadlocked")
	}
}

// --- Health ---

// TestHealthStatus проверяет корректность снимка состояния.
func TestHealthStatus(t *testing.T) {
	t.Parallel()

	tenantA := uuid.New()
	tenantB := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{
		&mockTenant{id: tenantA, limit: 2},
		&mockTenant{id: tenantB, limit: 5},
	})

	cfg := newTestConfig()
	m := startManager(t, provider, cfg)

	h := m.Health()

	if !h.Healthy {
		t.Error("Health.Healthy: want true")
	}

	if h.Stopping {
		t.Error("Health.Stopping: want false")
	}

	if h.PoolWorkerCount != cfg.WorkerCount {
		t.Errorf("Health.PoolWorkerCount: got %d, want %d", h.PoolWorkerCount, cfg.WorkerCount)
	}

	if h.PoolQueueCapacity != cfg.TaskQueueSize {
		t.Errorf("Health.PoolQueueCapacity: got %d, want %d", h.PoolQueueCapacity, cfg.TaskQueueSize)
	}

	if h.TenantCount != 2 {
		t.Errorf("Health.TenantCount: got %d, want 2", h.TenantCount)
	}

	byID := make(map[uuid.UUID]TenantHealth, len(h.Tenants))
	for _, th := range h.Tenants {
		byID[th.TenantID] = th
	}

	if th, ok := byID[tenantA]; !ok {
		t.Error("tenant A missing from Health.Tenants")
	} else if th.WorkerLimit != 2 {
		t.Errorf("tenant A WorkerLimit: got %d, want 2", th.WorkerLimit)
	}

	if th, ok := byID[tenantB]; !ok {
		t.Error("tenant B missing from Health.Tenants")
	} else if th.WorkerLimit != 5 {
		t.Errorf("tenant B WorkerLimit: got %d, want 5", th.WorkerLimit)
	}
}

// TestGetTenantIDs проверяет, что GetTenantIDs возвращает
// корректный снимок идентификаторов без дублей.
func TestGetTenantIDs(t *testing.T) {
	t.Parallel()

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}

	tenants := make([]Tenant, len(ids))
	for i, id := range ids {
		tenants[i] = &mockTenant{id: id, limit: 1}
	}

	provider := &mockTenantProvider{}
	provider.set(tenants)
	m := startManager(t, provider, newTestConfig())

	active := m.GetTenantIDs()
	if len(active) != len(ids) {
		t.Fatalf("GetTenantIDs: got %d, want %d", len(active), len(ids))
	}

	idSet := make(map[uuid.UUID]bool)
	for _, id := range active {
		idSet[id] = true
	}

	for _, id := range ids {
		if !idSet[id] {
			t.Errorf("tenant %s missing from GetTenantIDs", id)
		}
	}
}

// --- Регрессионные тесты для непокрытых ветвей (docs/TESTING_AUDIT.md) ---

// TestStartInitialRefreshFailure проверяет, что Start возвращает ошибку,
// если начальный refreshTenants не может получить список тенантов, и что
// пул, запущенный до этой проверки, корректно останавливается, а не
// остаётся висеть в фоне.
func TestStartInitialRefreshFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("provider unavailable")
	provider := &mockTenantProvider{}
	provider.setErr(wantErr)

	m, err := NewWorkerManager(WorkerManagerParams{TenantProvider: provider, Config: newTestConfig()})
	if err != nil {
		t.Fatalf("NewWorkerManager: %v", err)
	}

	if err := m.Start(); !errors.Is(err, wantErr) {
		t.Errorf("Start() error = %v, want errors.Is(err, wantErr)", err)
	}

	// Start должен был остановить уже запущенный пул перед возвратом ошибки:
	// последующий addTask обязан немедленно вернуть ErrPoolStopping, а не
	// зависнуть или тихо принять задачу, которую уже некому выполнять.
	if err := m.pool.addTask(newTask(uuid.New(), successExec(), func(error) {})); !errors.Is(err, ErrPoolStopping) {
		t.Errorf("pool.addTask after failed Start: err = %v, want errors.Is(err, ErrPoolStopping)", err)
	}
}

// TestRefreshTenantsSkipsAfterStop проверяет, что refreshTenants,
// вызванный после Stop, не воскрешает удалённые/новые тенанты — защита
// isStopping должна сработать раньше любых изменений w.tenants.
func TestRefreshTenantsSkipsAfterStop(t *testing.T) {
	t.Parallel()

	keptID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: keptID, limit: 1}})
	m := startManager(t, provider, newTestConfig())

	m.Stop()

	newID := uuid.New()
	provider.set([]Tenant{&mockTenant{id: keptID, limit: 1}, &mockTenant{id: newID, limit: 1}})

	if err := m.refreshTenants(); err != nil {
		t.Fatalf("refreshTenants after Stop: %v", err)
	}

	for _, id := range m.GetTenantIDs() {
		if id == newID {
			t.Error("refreshTenants added a new tenant after Stop")
		}
	}
}

// TestRefreshTenantsSkipsNilID проверяет, что тенант с нулевым UUID
// пропускается при построении множества тенантов, а не порождает
// пустой/некорректный tenantState.
func TestRefreshTenantsSkipsNilID(t *testing.T) {
	t.Parallel()

	goodID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{
		&mockTenant{id: uuid.Nil, limit: 1},
		&mockTenant{id: goodID, limit: 1},
	})
	m := startManager(t, provider, newTestConfig())

	ids := m.GetTenantIDs()
	if len(ids) != 1 || ids[0] != goodID {
		t.Errorf("GetTenantIDs = %v, want only [%v] (nil-id tenant must be skipped)", ids, goodID)
	}
}

// TestRefreshTenantsDefaultsNonPositiveWorkerLimit проверяет, что тенант с
// WorkerLimit <= 0 получает лимит по умолчанию (1), а не семафор нулевой
// ёмкости, который заблокировал бы диспетчер навсегда.
func TestRefreshTenantsDefaultsNonPositiveWorkerLimit(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 0}})
	m := startManager(t, provider, newTestConfig())

	done := make(chan error, 1)
	task := newTask(tenantID, successExec(), func(err error) { done <- err })

	if err := m.SubmitTask(tenantID, task); err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}

	err := waitComplete(t, done, 3*time.Second,
		"task did not complete — non-positive WorkerLimit may not have defaulted to 1")
	if err != nil {
		t.Errorf("task error: %v", err)
	}
}

// TestTenantRefresherLogsErrorOnListFailure проверяет, что фоновый
// tenantRefresher логирует ошибку provider.List, а не проглатывает её молча.
func TestTenantRefresherLogsErrorOnListFailure(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	var buf syncBuffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))

	m, err := NewWorkerManager(WorkerManagerParams{
		TenantProvider: provider,
		Config:         newTestConfig(),
		Logger:         logger,
	})
	if err != nil {
		t.Fatalf("NewWorkerManager: %v", err)
	}

	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(m.Stop)

	provider.setErr(errors.New("boom"))

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(buf.String(), "tenant refresh failed") {
		time.Sleep(10 * time.Millisecond)
	}

	if !strings.Contains(buf.String(), "tenant refresh failed") {
		t.Error(`expected "tenant refresh failed" to be logged after provider.List started returning an error`)
	}
}

// TestSubmitTaskTenantShuttingDown проверяет ветку ErrTenantShuttingDown в
// SubmitTask: если контекст тенанта отменён, а очередь тенанта заполнена
// (так что отправка в неё не готова к выполнению), select обязан
// детерминированно выбрать case <-state.ctx.Done(), а не default.
func TestSubmitTaskTenantShuttingDown(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 1}})

	cfg := newTestConfig()
	cfg.TenantQueueSize = 1
	m := startManager(t, provider, cfg)

	blockRelease := make(chan struct{})
	defer close(blockRelease)

	blocker := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		<-blockRelease
		return nil
	}}

	// A: занимает единственный слот семафора (тенант limit=1); диспетчер
	// зависает в pool.addTask/Execute, ожидая blockRelease.
	if err := m.SubmitTask(tenantID, newTask(tenantID, blocker, func(error) {})); err != nil {
		t.Fatalf("submit A: %v", err)
	}

	time.Sleep(30 * time.Millisecond) // дать диспетчеру подхватить A и вызвать pool.addTask

	// B: диспетчер читает B из очереди и блокируется на sem.Acquire, пока A
	// удерживает единственный слот — очередь тенанта снова пуста.
	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(error) {})); err != nil {
		t.Fatalf("submit B: %v", err)
	}

	time.Sleep(30 * time.Millisecond) // дать диспетчеру вычитать B и заблокироваться на sem.Acquire

	// C: заполняет единственный слот буфера очереди тенанта (cap=1).
	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(error) {})); err != nil {
		t.Fatalf("submit C: %v", err)
	}

	// Отменяем контекст именно этого тенанта напрямую (white-box: тот же
	// пакет), не трогая остальной WorkerManager — так же, как это делает
	// refreshTenants при удалении тенанта.
	m.tenantsMu.RLock()
	state := m.tenants[tenantID]
	m.tenantsMu.RUnlock()

	if state == nil {
		t.Fatal("tenant state not found")
	}

	state.cancel()

	err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(error) {}))
	if !errors.Is(err, ErrTenantShuttingDown) {
		t.Errorf("SubmitTask after tenant ctx cancelled: err = %v, want errors.Is(err, ErrTenantShuttingDown)", err)
	}
}

// TestDispatchWarnsOnPoolRejectionWithNoCompletionHandler проверяет ветку
// dispatch, где пул отклоняет задачу (переполнение taskChan) и у задачи нет
// Complete-обработчика: диспетчер обязан залогировать предупреждение, а не
// потерять ошибку молча.
func TestDispatchWarnsOnPoolRejectionWithNoCompletionHandler(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 3}})

	var buf syncBuffer

	logger := slog.New(slog.NewTextHandler(&buf, nil))

	cfg := newTestConfig()
	cfg.TaskQueueSize = 1
	cfg.WorkerCount = 1

	m, err := NewWorkerManager(WorkerManagerParams{TenantProvider: provider, Config: cfg, Logger: logger})
	if err != nil {
		t.Fatalf("NewWorkerManager: %v", err)
	}

	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(m.Stop)

	blockRelease := make(chan struct{})
	defer close(blockRelease)

	blocker := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		<-blockRelease
		return nil
	}}

	// Занимает единственного воркера пула — taskChan (cap=1) освобождается
	// сразу после того, как воркер его вычитает.
	if err := m.SubmitTask(tenantID, newTask(tenantID, blocker, func(error) {})); err != nil {
		t.Fatalf("submit blocker: %v", err)
	}

	time.Sleep(30 * time.Millisecond) // дать единственному воркеру вычитать blocker из taskChan

	// Заполняет единственный слот буфера taskChan пула.
	if err := m.SubmitTask(tenantID, newTask(tenantID, successExec(), func(error) {})); err != nil {
		t.Fatalf("submit filler: %v", err)
	}

	time.Sleep(30 * time.Millisecond) // дать диспетчеру протолкнуть filler в очередь пула

	// Задача без Complete — именно она должна вызвать addTask на полной
	// очереди пула и попасть в ветку original == nil.
	noCompleteTask := newTask(tenantID, successExec(), nil)
	if err := m.SubmitTask(tenantID, noCompleteTask); err != nil {
		t.Fatalf("submit no-complete task: %v", err)
	}

	const wantLog = "pool rejected task with no completion handler"

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(buf.String(), wantLog) {
		time.Sleep(10 * time.Millisecond)
	}

	if !strings.Contains(buf.String(), wantLog) {
		t.Errorf("expected %q to be logged", wantLog)
	}
}
