/*
Package workerpool provides a tenant-aware worker pool for executing tasks with per-tenant concurrency limits.

# Overview

WorkerPool manages task execution across multiple tenants, ensuring that each tenant gets its
allocated share of workers and cannot exceed its concurrency limit. It consists of three main components:

1. WorkerManager - Per-tenant scheduling and concurrency control
2. Pool - Global worker pool for actual task execution
3. Task - Unit of work that can be executed

# Core Concepts

Tenant:
  - Entities that require isolated task execution
  - Each tenant has its own worker limit
  - Tasks are triggered per tenant

Worker Limit:
  - Maximum number of concurrent tasks per tenant
  - Can be changed dynamically at runtime
  - Enforced via semaphore pattern

Task Execution:
  - Tasks are first queued per tenant
  - Tenant workers pull tasks and submit to global pool
  - Global pool handles retries with exponential backoff

# Usage Example

	type MyTenant struct {
		id    uuid.UUID
		limit int
	}

	func (t MyTenant) GetID() uuid.UUID { return t.id }
	func (t MyTenant) GetWorkerLimit() int { return t.limit }

	type MyExecutor struct{}

	func (e MyExecutor) Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error {
		// actual work here
		return nil
	}

	func main() {
		config := workerpool.Config{
			TaskTimeout: 5 * time.Minute,
			PoolSize: workerpool.PoolSize{Normal: 32},
		}

		manager, err := workerpool.NewWorkerManager(workerpool.WorkerManagerParams{
			Logger:         slog.Default(),
			TenantProvider: myProvider,
			TaskExecutor:   &MyExecutor{},
			Config:         config,
		})

		if err != nil {
			log.Fatal(err)
		}

		manager.Start()
		defer manager.Stop()

		// Trigger task for specific tenant
		manager.Trigger(tenantID)
	}

# Concurrency Model

  - Per-tenant: N workers (where N = tenant limit)
  - Global pool: M workers (configurable)
  - Total concurrency = sum(tenant limits) but bounded by global pool size

# Error Handling

  - Panics are recovered at all levels
  - Tasks are retried with exponential backoff
  - Queue overflow leads to task drop with warning
  - All errors are logged with context

# Thread Safety

All public methods are safe for concurrent use.
*/
package workerpool
