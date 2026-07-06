package workerpool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// mockTenant — минимальная реализация Tenant для тестов.
type mockTenant struct {
	id    uuid.UUID
	limit int
}

var _ Tenant = (*mockTenant)(nil)

func (m *mockTenant) ID() uuid.UUID    { return m.id }
func (m *mockTenant) WorkerLimit() int { return m.limit }

// mockTenantProvider — потокобезопасная реализация TenantProvider для тестов.
type mockTenantProvider struct {
	mu      sync.Mutex
	tenants []Tenant
	err     error
}

var _ TenantProvider = (*mockTenantProvider)(nil)

func (m *mockTenantProvider) List(_ context.Context) ([]Tenant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.tenants, m.err
}

func (m *mockTenantProvider) set(tenants []Tenant) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tenants = tenants
}

// mockExecutor — реализация TaskExecutor с настраиваемым поведением.
type mockExecutor struct {
	fn func(ctx context.Context, tenantID uuid.UUID, workerID int) error
}

var _ TaskExecutor = (*mockExecutor)(nil)

func (m *mockExecutor) Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error {
	return m.fn(ctx, tenantID, workerID)
}

// newTestConfig возвращает конфигурацию с минимальными таймаутами для быстрых тестов.
func newTestConfig() Config {
	return Config{
		WorkerCount:           16,
		TaskQueueSize:         64,
		TenantQueueSize:       16,
		GracefulTimeout:       2 * time.Second,
		TaskTimeout:           5 * time.Second,
		TenantRefreshInterval: 10 * time.Millisecond,
		RetryPolicy: RetryPolicy{
			Attempts: AttemptsConfig{
				Count:    1,
				MinDelay: time.Millisecond,
				MaxDelay: 10 * time.Millisecond,
			},
		},
	}
}

// startManager создаёт и запускает WorkerManager, регистрирует Stop в Cleanup.
// Принимает testing.TB, а не *testing.T, чтобы быть переиспользуемым как в
// тестах, так и в бенчмарках (*testing.B тоже реализует testing.TB).
func startManager(tb testing.TB, provider TenantProvider, cfg Config) *WorkerManager {
	tb.Helper()

	m, err := NewWorkerManager(WorkerManagerParams{TenantProvider: provider, Config: cfg})
	if err != nil {
		tb.Fatalf("NewWorkerManager: %v", err)
	}

	if err := m.Start(); err != nil {
		tb.Fatalf("Start: %v", err)
	}

	tb.Cleanup(m.Stop)

	return m
}

// waitSignal ждёт закрытия/отправки в ch не дольше timeout, иначе — t.Fatal(msg).
func waitSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

// waitComplete ждёт значение из ch не дольше timeout, иначе — t.Fatal(msg).
func waitComplete(t *testing.T, ch <-chan error, timeout time.Duration, msg string) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		t.Fatal(msg)
		return nil
	}
}

// newTask формирует Task с context.Background() и новым TaskID.
func newTask(tenantID uuid.UUID, exec TaskExecutor, complete func(error)) Task {
	return Task{
		Ctx:      context.Background(),
		TaskID:   uuid.New(),
		TenantID: tenantID,
		Executor: exec,
		Complete: complete,
	}
}

// successExec возвращает executor, который немедленно возвращает nil.
func successExec() *mockExecutor {
	return &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error { return nil }}
}
