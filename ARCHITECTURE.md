# workerpool — Архитектура

## Обзор

`workerpool` — движок исполнения задач с изоляцией по тенантам, спроектированный
для multi-instance развёртывания, где задачи должны выполняться в рамках
per-tenant лимитов конкурентности и переживать перезапуск процесса.

Пакет намеренно отвечает только за **исполнение**. Персистентное планирование,
cron-выражения и координация между инстансами делегируются
[River](https://github.com/riverqueue/river) — Postgres-backed job queue.

---

## Карта компонентов

```
┌─────────────────────────────────────────────────────────────┐
│                        Postgres                              │
│  river_jobs { id, kind, args, cron, state, tenant_id, … }  │
└──────────────┬──────────────────────────────────────────────┘
               │  SELECT … FOR UPDATE SKIP LOCKED
               │  (безопасно для нескольких инстансов; leader election для cron)
┌──────────────▼──────────────────────────────────────────────┐
│                         River                                │
│  • PeriodicJob  — cron-расписание хранится в Postgres        │
│  • InsertMany   — fan-out: по одному job на каждого тенанта  │
│  • UniqueOpts   — защита от дублирования job по тенанту      │
│  • Dead-letter  — упавшие job после исчерпания попыток       │
└──────────────┬──────────────────────────────────────────────┘
               │  river.Worker.Work() → WorkerManager.SubmitTask()
┌──────────────▼──────────────────────────────────────────────┐
│                     WorkerManager                            │
│  tenants: map[uuid]*tenantState                              │
│    tenantState:                                              │
│      taskQueue  chan Task   (буфер TenantQueueSize)          │
│      sem        *semaphore.Weighted  (= WorkerLimit)         │
│      dispatcher 1 горутина                                   │
│  TenantProvider → обновляется каждые TenantRefreshInterval   │
└──────────────┬──────────────────────────────────────────────┘
               │  pool.addTask()
┌──────────────▼──────────────────────────────────────────────┐
│                          Pool                                │
│  WorkerCount горутин                                         │
│  OTel-трассировка (span на каждый вызов Execute)             │
│  OTel-метрики (гистограмма длительности, счётчик завершений) │
│  Повторные попытки с экспоненциальным backoff                │
│  Восстановление после паники + перезапуск воркера            │
│  GracefulTimeout → forceCancel с распространением в контексты│
└─────────────────────────────────────────────────────────────┘
```

---

## Модель конкурентности

| Уровень | Горутины | Назначение |
|---|---|---|
| Pool | `WorkerCount` (фиксировано) | Исполнение задач |
| Диспетчер тенанта | 1 на активного тенанта | Соблюдение лимита через семафор |
| Обновлятор тенантов | 1 | Периодическая синхронизация с TenantProvider |

Одновременно в пуле выполняется не более `WorkerLimit` задач одного тенанта.
Диспетчер блокируется на `sem.Acquire` до освобождения слота и не занимает
слот пула в ожидании.

**Бюджет горутин** — 200 тенантов, `WorkerCount=256`:

| Компонент | Горутины |
|---|---|
| Воркеры пула | 256 |
| Диспетчеры тенантов | 200 |
| Обновлятор | 1 |
| **Итого** | **457** |

Это замена прежней схемы (N блокирующих горутин на тенанта), которая требовала
до 200 × avg_limit горутин только для диспетчеров.

---

## Жизненный цикл задачи

```
Вызывающий код / River Worker
    │
    ▼ SubmitTask()
tenant.taskQueue  ──(буферизованный chan)──►  горутина-диспетчер
                                                      │
                                              sem.Acquire()   ← блокируется при исчерпании лимита
                                                      │
                                              pool.addTask()
                                                      │
                                          горутина-воркер пула
                                                      │
                                          Executor.Execute()  ← OTel span
                                                      │ (повтор при ошибке)
                                          task.Complete(err)
                                                      │
                                              sem.Release()
                                                      │
                                          River Worker разблокируется
                                              (возвращает nil/err в River)
```

---

## Режимы диспетчеризации

На уровне **River / приложения** поддерживаются два паттерна запуска задач.

### AllTenants (широковещательный)

Cron-job выполняет fan-out: вставляет по одному River job на каждого активного
тенанта. Leader election River гарантирует, что fan-out выполняется ровно один
раз за период на весь кластер.

```go
// Fan-out River worker — запускается по cron, вставляет по одному job на тенанта.
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

// Регистрация как periodic job (cron).
river.NewPeriodicJob(
    river.PeriodicInterval(20 * time.Second),
    func() (river.JobArgs, *river.InsertOpts) {
        return FanOutArgs{Kind: "sync_orders"}, nil
    },
    &river.PeriodicJobOpts{RunOnStart: true},
)
```

### OneTenant (адресный)

Вставка одного River job с целевым `TenantID` в args. Работает из любого
контекста: HTTP-обработчика, консьюмера событий, другого воркера.

```go
// Из HTTP-обработчика (в рамках бизнес-транзакции):
_, err := riverClient.InsertTx(ctx, tx, SyncOrdersArgs{TenantID: tenantID}, nil)
```

---

## Защита от дублирования (UniqueOpts)

Для задач, где повторное выполнение влечёт бизнес-сбой, используйте
`UniqueOpts` River для ограничения «не более одного активного job на тенанта»:

```go
InsertOpts: &river.InsertOpts{
    UniqueOpts: river.UniqueOpts{
        ByArgs:  true,   // ключ уникальности = хэш args (включая TenantID)
        ByState: []rivertype.JobState{
            rivertype.JobStateAvailable,
            rivertype.JobStateRunning,
        },
        // Без ByPeriod — уникальность действует, пока job в этих состояниях.
    },
},
```

Цепочка гарантий:
1. **Leader election River** — fan-out выполняется ровно один раз за период на весь кластер.
2. **UniqueOpts** — не более одного job на тенанта в состоянии `available` или `running`.
3. **Семафор WorkerManager** — не более `WorkerLimit` одновременных выполнений тенанта в пуле.

---

## Паттерн интеграции River worker

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
        return err // River повторит попытку
    }

    select {
    case err := <-done:
        return err // nil → River помечает job выполненным; non-nil → River ретраит
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

При использовании River для повторных попыток установите
`RetryPolicy.Attempts.Count = 1` в Config пула, чтобы избежать двойного retry.

---

## OpenTelemetry

Пул эмитирует следующие инструменты под именем `workerpool`:

| Тип | Имя | Метки |
|---|---|---|
| `Float64Histogram` | `workerpool.task.duration` | `tenant.id`, `status` |
| `Int64Counter` | `workerpool.tasks.total` | `tenant.id`, `status` |
| `Span` | `workerpool.task.execute` | `tenant.id`, `task.id`, `worker.id`, `attempt` |

`status` принимает значения `success`, `failed`, `cancelled`.

Провайдеры берутся из глобальных OTel-провайдеров: настройте
`otel.SetTracerProvider` и `otel.SetMeterProvider` до старта менеджера.
При отсутствии настройки используется noop-реализация.

---

## Справочник конфигурации

| Поле | По умолчанию | Описание |
|---|---|---|
| `WorkerCount` | 256 | Горутины глобального пула |
| `TaskQueueSize` | 512 | Буфер канала глобального пула |
| `TenantQueueSize` | 64 | Буфер канала на тенанта |
| `GracefulTimeout` | 30s | Максимальное ожидание до force-cancel при Stop() |
| `TaskTimeout` | 5m | Дедлайн задачи по умолчанию |
| `TenantRefreshInterval` | 30s | Период опроса TenantProvider |
| `RetryPolicy.Attempts.Count` | 3 | Максимум попыток (установите 1 при использовании River) |
| `RetryPolicy.Attempts.MinDelay` | 1s | Начальная пауза backoff |
| `RetryPolicy.Attempts.MaxDelay` | 30s | Максимальная пауза backoff |

Все значения валидируются через `Config.Validate()`, который вызывается внутри
`NewWorkerManager`.

---

## Последовательность graceful shutdown

```
WorkerManager.Stop()
    │
    ├─ stopping = true
    ├─ отмена контекста менеджера    → останавливает горутину tenantRefresher
    ├─ отмена контекстов тенантов    → останавливает все горутины-диспетчеры
    ├─ wg.Wait()                     → ожидание завершения диспетчеров и обновлятора
    │
    └─ pool.stop()
        ├─ close(taskChan)           → воркеры вычитывают оставшиеся задачи
        ├─ ожидание до GracefulTimeout
        └─ [по таймауту] forceCancel() → распространяется в контексты всех задач
                         wg.Wait()     → ожидание завершения всех горутин пула
```

---

## Чеклист для продакшена

- [ ] Настроить OTel `MeterProvider` и `TracerProvider` (Prometheus / Jaeger / OTLP).
- [ ] Подключить `WorkerManager.Health()` к эндпоинтам `/healthz` и `/readyz`.
- [ ] Установить `UniqueOpts` для всех задач, где дублирование недопустимо.
- [ ] Установить `RetryPolicy.Attempts.Count = 1`, если повторными попытками управляет River.
- [ ] `WorkerCount` ≥ ожидаемого пикового числа одновременных задач по всем тенантам.
- [ ] `TenantQueueSize` должен поглощать всплески без потери задач.
- [ ] Мониторить `workerpool.task.duration` P95/P99 для выявления деградации.
- [ ] Мониторить `workerpool.tasks.total{status="failed"}` для алертинга по ошибкам.
- [ ] Мониторить соотношение `HealthStatus.PoolQueueDepth / PoolQueueCapacity` для обнаружения backpressure.
