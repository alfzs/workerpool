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

func (m *mockTenant) GetID() uuid.UUID    { return m.id }
func (m *mockTenant) GetWorkerLimit() int { return m.limit }

// mockTenantProvider — потокобезопасная реализация TenantProvider для тестов.
type mockTenantProvider struct {
	mu      sync.Mutex
	tenants []Tenant
	err     error
}

func (m *mockTenantProvider) GetActive(_ context.Context) ([]Tenant, error) {
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
func startManager(t *testing.T, provider TenantProvider, cfg Config) *WorkerManager {
	t.Helper()
	m, err := NewWorkerManager(WorkerManagerParams{TenantProvider: provider, Config: cfg})
	if err != nil {
		t.Fatalf("NewWorkerManager: %v", err)
	}
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(m.Stop)
	return m
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
