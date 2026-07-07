package workerpool_test

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	workerpool "github.com/alfzs/workerpool/v2"
)

// echoExecutor is a minimal TaskExecutor that always succeeds.
type echoExecutor struct{}

var _ workerpool.TaskExecutor = echoExecutor{}

func (echoExecutor) Execute(_ context.Context, _ uuid.UUID, _ int) error { return nil }

// ExampleExecutorRegistry demonstrates registering a TaskExecutor under a
// string key and resolving it back at execution time — the pattern used
// when a job store (e.g. River) persists only the key.
func ExampleExecutorRegistry() {
	registry := workerpool.NewExecutorRegistry()
	registry.MustRegister("echo", echoExecutor{})

	exec, err := registry.Get("echo")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(exec.Execute(context.Background(), uuid.Nil, 0))
	// Output: <nil>
}

// staticTenant is a fixed-limit Tenant implementation for demonstration
// purposes; a real implementation would back WorkerLimit with a value
// that can change between refresh cycles.
type staticTenant struct {
	id    uuid.UUID
	limit int
}

var _ workerpool.Tenant = staticTenant{}

func (t staticTenant) ID() uuid.UUID    { return t.id }
func (t staticTenant) WorkerLimit() int { return t.limit }

// staticProvider is a TenantProvider returning a fixed tenant set; a real
// implementation should cache its own results, since List is called
// on every TenantRefreshInterval tick.
type staticProvider struct{ tenants []workerpool.Tenant }

var _ workerpool.TenantProvider = staticProvider{}

func (p staticProvider) List(_ context.Context) ([]workerpool.Tenant, error) {
	return p.tenants, nil
}

// ExampleNewWorkerManager builds a manager with a single tenant, submits one
// task, and waits for it to complete via the Task.Complete callback.
func ExampleNewWorkerManager() {
	tenantID := uuid.New()
	provider := staticProvider{tenants: []workerpool.Tenant{staticTenant{id: tenantID, limit: 1}}}

	cfg := workerpool.Config{
		WorkerCount:           2,
		TaskQueueSize:         8,
		TenantQueueSize:       8,
		GracefulTimeout:       time.Second,
		TaskTimeout:           time.Second,
		TenantRefreshInterval: time.Hour,
		RetryPolicy: workerpool.RetryPolicy{
			Attempts: workerpool.AttemptsConfig{
				Count:    1,
				MinDelay: time.Millisecond,
				MaxDelay: time.Millisecond,
			},
		},
	}

	// NewWorkerManager calls cfg.Validate() internally.
	manager, err := workerpool.NewWorkerManager(workerpool.WorkerManagerParams{
		TenantProvider: provider,
		Config:         cfg,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	// Start loads the initial tenant set synchronously, so the tenant is
	// already known by the time SubmitTask is called below.
	if err := manager.Start(); err != nil {
		fmt.Println(err)
		return
	}
	defer manager.Stop()

	done := make(chan error, 1)

	err = manager.SubmitTask(tenantID, workerpool.Task{
		Ctx:      context.Background(),
		TaskID:   uuid.New(),
		TenantID: tenantID,
		Executor: echoExecutor{},
		Complete: func(err error) { done <- err },
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	select {
	case err := <-done:
		fmt.Println(err)
	case <-time.After(5 * time.Second):
		fmt.Println("timed out waiting for task completion")
	}
	// Output: <nil>
}
