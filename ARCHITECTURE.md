# workerpool — Architecture

## Overview

`workerpool` is a tenant-aware execution engine designed for multi-instance
deployments where tasks must run within per-tenant concurrency limits and
survive process restarts.

The package deliberately handles **execution** only. Persistent scheduling,
cron expressions, and multi-instance coordination are delegated to
[River](https://github.com/riverqueue/river) — a Postgres-backed job queue.

---

## Component map

```
┌─────────────────────────────────────────────────────────────┐
│                        Postgres                              │
│  river_jobs { id, kind, args, cron, state, tenant_id, … }  │
└──────────────┬──────────────────────────────────────────────┘
               │  SELECT … FOR UPDATE SKIP LOCKED
               │  (multi-instance safe; leader election for cron)
┌──────────────▼──────────────────────────────────────────────┐
│                         River                                │
│  • PeriodicJob  – cron scheduling stored in Postgres         │
│  • InsertMany   – fan-out: one job per active tenant         │
│  • UniqueOpts   – prevents duplicate jobs per tenant         │
│  • Dead-letter  – failed jobs after max retries              │
└──────────────┬──────────────────────────────────────────────┘
               │  river.Worker.Work() → WorkerManager.SubmitTask()
┌──────────────▼──────────────────────────────────────────────┐
│                     WorkerManager                            │
│  tenants: map[uuid]*tenantState                              │
│    tenantState:                                              │
│      taskQueue  chan Task   (TenantQueueSize buffer)         │
│      sem        *semaphore.Weighted  (= WorkerLimit)         │
│      dispatcher 1 goroutine                                  │
│  TenantProvider → refreshed every TenantRefreshInterval      │
└──────────────┬──────────────────────────────────────────────┘
               │  pool.addTask()
┌──────────────▼──────────────────────────────────────────────┐
│                          Pool                                │
│  WorkerCount goroutines                                      │
│  OTel tracing (span per Execute call)                        │
│  OTel metrics (duration histogram, completion counter)       │
│  Exponential backoff retry (configurable)                    │
│  Panic recovery + worker restart                             │
│  GracefulTimeout → forceCancel on context propagation        │
└─────────────────────────────────────────────────────────────┘
```

---

## Concurrency model

| Layer | Goroutines | Purpose |
|---|---|---|
| Pool | `WorkerCount` (fixed) | Execute tasks |
| Tenant dispatcher | 1 per active tenant | Enforce per-tenant limit via semaphore |
| Tenant refresh | 1 | Periodically sync TenantProvider |

At most `WorkerLimit` tasks per tenant run in the pool simultaneously.
The dispatcher blocks on `sem.Acquire` until a slot is free, then immediately
releases itself from the queue — it does not consume a pool slot while waiting.

**Goroutine budget example** — 200 tenants, `WorkerCount=256`:

| Component | Goroutines |
|---|---|
| Pool workers | 256 |
| Tenant dispatchers | 200 |
| Refresher | 1 |
| **Total** | **457** |

This replaces the previous design (N blocking goroutines per tenant) which
would have required up to 200 × avg_limit goroutines for dispatchers alone.

---

## Task lifecycle

```
Caller / River Worker
    │
    ▼ SubmitTask()
tenant.taskQueue  ──(buffered chan)──►  dispatcher goroutine
                                              │
                                        sem.Acquire()   ← blocks if at limit
                                              │
                                        pool.addTask()
                                              │
                                    pool worker goroutine
                                              │
                                    Executor.Execute()  ← OTel span
                                              │ (retry on error)
                                    task.Complete(err)
                                              │
                                        sem.Release()
                                              │
                                    River Worker unblocks
                                        (returns nil/err to River)
```

---

## Dispatch modes

Two task dispatch patterns are supported at the **River / application** layer:

### AllTenants (broadcast)

A cron job fans out one River job per active tenant.
River's leader election ensures the fan-out runs exactly once per period
across all instances.

```go
// Fan-out River worker — runs per cron, inserts one job per tenant.
type FanOutWorker struct {
    riverClient *river.Client[pgx.Tx]
    manager     *workerpool.WorkerManager
}

func (w *FanOutWorker) Work(ctx context.Context, job *river.Job[FanOutArgs]) error {
    tenants := w.manager.GetActiveTenants()
    batch := make([]river.InsertManyParams, len(tenants))
    for i, tid := range tenants {
        batch[i] = river.InsertManyParams{
            Args: SyncOrdersArgs{TenantID: tid},
            InsertOpts: &river.InsertOpts{
                UniqueOpts: river.UniqueOpts{
                    ByArgs:  true,
                    ByState: []rivertype.JobState{
                        rivertype.JobStateAvailable,
                        rivertype.JobStateRunning,
                    },
                },
            },
        }
    }
    _, err := w.riverClient.InsertMany(ctx, batch)
    return err
}

// Register as a periodic job (cron).
river.NewPeriodicJob(
    river.PeriodicInterval(20 * time.Second),
    func() (river.JobArgs, *river.InsertOpts) {
        return FanOutArgs{Kind: "sync_orders"}, nil
    },
    &river.PeriodicJobOpts{RunOnStart: true},
)
```

### OneTenant (targeted)

Insert a single River job with the target `TenantID` in args.
Works from any context: HTTP handler, event consumer, another worker.

```go
// From an HTTP handler (within a business transaction):
_, err := riverClient.InsertTx(ctx, tx, SyncOrdersArgs{TenantID: tenantID}, nil)
```

---

## Preventing duplicate execution (UniqueOpts)

For tasks where duplicate execution causes business failures, use River's
`UniqueOpts` to enforce at-most-one-in-flight per tenant:

```go
InsertOpts: &river.InsertOpts{
    UniqueOpts: river.UniqueOpts{
        ByArgs:  true,   // uniqueness key = hash of job args (incl. TenantID)
        ByState: []rivertype.JobState{
            rivertype.JobStateAvailable,
            rivertype.JobStateRunning,
        },
        // No ByPeriod = unique for as long as the job is in those states.
    },
},
```

Guarantee chain:
1. **River leader election** — fan-out executes once per period across instances.
2. **UniqueOpts** — at most one job per tenant in `available` or `running` state.
3. **WorkerManager semaphore** — at most `WorkerLimit` concurrent executions per tenant in the pool.

---

## River worker integration pattern

```go
type SyncOrdersWorker struct {
    manager  *workerpool.WorkerManager
    registry *workerpool.ExecutorRegistry
}

func (w *SyncOrdersWorker) Work(ctx context.Context, job *river.Job[SyncOrdersArgs]) error {
    exec, err := w.registry.Get("sync_orders")
    if err != nil {
        return err
    }

    done := make(chan error, 1)

    err = w.manager.SubmitTask(job.Args.TenantID, workerpool.Task{
        Ctx:      ctx,
        TaskID:   uuid.MustParse(job.ID.String()),
        TenantID: job.Args.TenantID,
        Executor: exec,
        Complete: func(err error) { done <- err },
    })
    if err != nil {
        return err // River will retry
    }

    select {
    case err := <-done:
        return err // nil → River marks complete; non-nil → River retries
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

When using River for retries, set `RetryPolicy.Attempts.Count = 1` in the
workerpool Config to avoid double-retry.

---

## OpenTelemetry

The pool emits the following instruments under the `workerpool` meter/tracer:

| Type | Name | Labels |
|---|---|---|
| `Float64Histogram` | `workerpool.task.duration` | `tenant.id`, `status` |
| `Int64Counter` | `workerpool.tasks.total` | `tenant.id`, `status` |
| `Span` | `workerpool.task.execute` | `tenant.id`, `task.id`, `worker.id`, `attempt` |

`status` is one of `success`, `failed`, `cancelled`.

Providers are injected via `WorkerManagerParams.TracerProvider` and
`WorkerManagerParams.MeterProvider`. When nil, the global OTel providers are
used (noop unless the application configures them).

---

## Configuration reference

| Field | Default | Description |
|---|---|---|
| `WorkerCount` | 256 | Global pool goroutines |
| `TaskQueueSize` | 512 | Global pool channel buffer |
| `TenantQueueSize` | 64 | Per-tenant channel buffer |
| `GracefulTimeout` | 30s | Max wait before force-cancel on Stop() |
| `TaskTimeout` | 5m | Default task context deadline |
| `TenantRefreshInterval` | 30s | How often TenantProvider is polled |
| `RetryPolicy.Attempts.Count` | 3 | Max attempts per task (set to 1 with River) |
| `RetryPolicy.Attempts.MinDelay` | 1s | Initial retry backoff |
| `RetryPolicy.Attempts.MaxDelay` | 30s | Max retry backoff |

All values are validated by `Config.Validate()`, which is called inside
`NewWorkerManager`.

---

## Graceful shutdown sequence

```
WorkerManager.Stop()
    │
    ├─ stopping = true
    ├─ cancel manager ctx          → stops tenantRefresher goroutine
    ├─ cancel all tenant ctxs      → stops all dispatcher goroutines
    ├─ wg.Wait()                   → wait for all dispatchers + refresher
    │
    └─ pool.stop()
        ├─ close(taskChan)         → workers drain remaining tasks
        ├─ wait up to GracefulTimeout
        └─ [on timeout] forceCancel() → propagates into all task contexts
                        wg.Wait()     → wait for all pool goroutines
```

---

## Operational checklist for production

- [ ] Configure OTel `MeterProvider` and `TracerProvider` (Prometheus / Jaeger / OTLP).
- [ ] Wire `WorkerManager.Health()` to `/healthz` and `/readyz` endpoints.
- [ ] Set `UniqueOpts` on all tasks where duplicate execution is harmful.
- [ ] Set `RetryPolicy.Attempts.Count = 1` when River handles outer retries.
- [ ] Size `WorkerCount` ≥ expected peak concurrent tasks across all tenants.
- [ ] Size `TenantQueueSize` to absorb burst submissions without dropping tasks.
- [ ] Monitor `workerpool.task.duration` P95/P99 for latency regressions.
- [ ] Monitor `workerpool.tasks.total{status="failed"}` for error rate alerting.
- [ ] Monitor `HealthStatus.PoolQueueDepth` / `PoolQueueCapacity` ratio for backpressure.
