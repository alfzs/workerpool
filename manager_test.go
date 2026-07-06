package workerpool

import (
	"context"
	"errors"
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
