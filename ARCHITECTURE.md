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

## Полный пример интеграции с River

Сквозной пример: от миграций и определения типов до запуска и graceful shutdown.

### Зависимости

```bash
go get github.com/riverqueue/river
go get github.com/riverqueue/river/riverdriver/riverpgxv5
go get github.com/jackc/pgx/v5
```

### Миграции

River хранит всё состояние в Postgres. Миграции прогоняются один раз до старта
приложения — при повторном запуске они идемпотентны.

```go
import (
    "github.com/riverqueue/river/riverdriver/riverpgxv5"
    "github.com/riverqueue/river/rivermigrate"
)

func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
    migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
    if err != nil {
        return err
    }
    _, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
    return err
}
```

### Типы job

Каждый тип задачи — отдельная структура, реализующая `river.JobArgs`.
Метод `Kind()` возвращает строку, используемую также как ключ в `ExecutorRegistry`.

```go
// SyncOrdersArgs — аргументы задачи синхронизации заказов конкретного тенанта.
type SyncOrdersArgs struct {
    TenantID uuid.UUID `json:"tenant_id"`
}

func (SyncOrdersArgs) Kind() string { return "sync_orders" }

// FanOutArgs — триггер для режима AllTenants: один job запускает fan-out.
type FanOutArgs struct {
    JobKind string `json:"job_kind"`
}

func (FanOutArgs) Kind() string { return "fan_out" }
```

### Реализация TaskExecutor

Бизнес-логика инкапсулирована в `TaskExecutor`. River и workerpool о ней не знают.

```go
type SyncOrdersExecutor struct {
    db  *pgxpool.Pool
    api *OrdersAPIClient
}

func (e *SyncOrdersExecutor) Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error {
    orders, err := e.api.FetchPending(ctx, tenantID)
    if err != nil {
        return fmt.Errorf("fetch orders: %w", err)
    }
    return e.db.SaveOrders(ctx, tenantID, orders)
}
```

### River Worker — адаптер к workerpool

River Worker — тонкий слой: получает job из Postgres, передаёт в workerpool,
блокируется до получения результата. Ошибка из `done` возвращается в River,
который принимает решение о повторной попытке.

```go
type SyncOrdersWorker struct {
    manager  *workerpool.WorkerManager
    registry *workerpool.ExecutorRegistry
}

func (w *SyncOrdersWorker) Work(ctx context.Context, job *river.Job[SyncOrdersArgs]) error {
    exec, err := w.registry.Get(job.Args.Kind())
    if err != nil {
        // Executor не зарегистрирован — программная ошибка, не ретраить.
        return river.JobCancel(err)
    }

    done := make(chan error, 1)

    err = w.manager.SubmitTask(job.Args.TenantID, workerpool.Task{
        Ctx:      ctx, // контекст River: несёт таймаут и OTel-span вызывающего кода
        TaskID:   uuid.New(),
        TenantID: job.Args.TenantID,
        Executor: exec,
        Complete: func(err error) { done <- err },
    })
    if err != nil {
        // Очередь тенанта заполнена или тенант неизвестен — River повторит позже.
        return fmt.Errorf("submit task: %w", err)
    }

    select {
    case taskErr := <-done:
        return taskErr // nil → job выполнен; non-nil → River ретраит
    case <-ctx.Done():
        // River останавливается или истёк JobTimeout.
        // Job возвращается в очередь со статусом retryable.
        return ctx.Err()
    }
}
```

### Fan-out Worker (режим AllTenants)

Fan-out Worker запускается по cron и вставляет по одному job на каждого активного
тенанта. `UniqueOpts` гарантирует, что если предыдущий job тенанта ещё не
завершился — новый молча игнорируется.

```go
type FanOutWorker struct {
    riverClient *river.Client[pgx.Tx]
    manager     *workerpool.WorkerManager
}

func (w *FanOutWorker) Work(ctx context.Context, job *river.Job[FanOutArgs]) error {
    tenants := w.manager.GetActiveTenants()
    if len(tenants) == 0 {
        return nil
    }

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
```

### Сборка в main

```go
func main() {
    ctx := context.Background()

    // База данных.
    dbPool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatal(err)
    }
    defer dbPool.Close()

    // Миграции River — идемпотентны при повторных запусках.
    if err := runMigrations(ctx, dbPool); err != nil {
        log.Fatal(err)
    }

    // Реестр executor'ов: ключи соответствуют Kind() типов job.
    registry := workerpool.NewExecutorRegistry()
    registry.MustRegister("sync_orders", &SyncOrdersExecutor{
        db:  dbPool,
        api: newOrdersAPIClient(),
    })

    // WorkerManager: RetryPolicy.Attempts.Count = 1, т.к. River управляет retry.
    manager, err := workerpool.NewWorkerManager(workerpool.WorkerManagerParams{
        TenantProvider: NewPostgresTenantProvider(dbPool),
        Config: workerpool.Config{
            WorkerCount:           256,
            TaskQueueSize:         512,
            TenantQueueSize:       32,
            GracefulTimeout:       30 * time.Second,
            TaskTimeout:           2 * time.Minute,
            TenantRefreshInterval: 30 * time.Second,
            RetryPolicy: workerpool.RetryPolicy{
                Attempts: workerpool.AttemptsConfig{Count: 1},
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    // Клиент River только для вставки job (без воркеров) — нужен FanOutWorker'у.
    insertClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{})
    if err != nil {
        log.Fatal(err)
    }

    // Регистрация воркеров River.
    workers := river.NewWorkers()
    river.AddWorker(workers, &SyncOrdersWorker{manager: manager, registry: registry})
    river.AddWorker(workers, &FanOutWorker{riverClient: insertClient, manager: manager})

    // Основной клиент River: воркеры + очереди + cron.
    //
    // MaxWorkers должен быть >= WorkerCount пула: каждый Work() блокируется
    // до завершения задачи, поэтому River нужно столько же слотов, сколько
    // задач может одновременно выполняться в пуле.
    riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
        Workers: workers,
        Queues: map[string]river.QueueConfig{
            river.QueueDefault: {MaxWorkers: 256},
        },
        PeriodicJobs: []*river.PeriodicJob{
            river.NewPeriodicJob(
                river.PeriodicInterval(20*time.Second),
                func() (river.JobArgs, *river.InsertOpts) {
                    return FanOutArgs{JobKind: "sync_orders"}, nil
                },
                &river.PeriodicJobOpts{RunOnStart: true},
            ),
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    // Старт.
    if err := manager.Start(); err != nil {
        log.Fatal(err)
    }
    if err := riverClient.Start(ctx); err != nil {
        log.Fatal(err)
    }

    // Graceful shutdown по сигналу ОС.
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
    <-quit

    // 1. Останавливаем River: ждёт завершения активных Work() вызовов.
    //    Work() разблокируется через done-канал или по ctx.Done().
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
    defer cancel()
    if err := riverClient.Stop(shutdownCtx); err != nil {
        slog.Error("river stop", "error", err)
    }

    // 2. Останавливаем workerpool: сливает очередь, при необходимости force-cancel.
    manager.Stop()
}
```

### Адресный запуск (режим OneTenant)

Из любого места приложения — HTTP-обработчика, gRPC, Kafka-консьюмера:

```go
// Вне транзакции:
_, err := riverClient.Insert(ctx, SyncOrdersArgs{TenantID: tenantID}, &river.InsertOpts{
    UniqueOpts: river.UniqueOpts{
        ByArgs:  true,
        ByState: []rivertype.JobState{
            rivertype.JobStateAvailable,
            rivertype.JobStateRunning,
        },
    },
})

// Внутри бизнес-транзакции (атомарно с изменением данных):
// Если транзакция откатится — job не появится в очереди.
_, err = riverClient.InsertTx(ctx, tx, SyncOrdersArgs{TenantID: tenantID}, &river.InsertOpts{
    UniqueOpts: river.UniqueOpts{
        ByArgs:  true,
        ByState: []rivertype.JobState{
            rivertype.JobStateAvailable,
            rivertype.JobStateRunning,
        },
    },
})
```

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

## Тесты

### Структура тестов

Тесты находятся в том же пакете `workerpool` (white-box), что даёт доступ к
неэкспортированным методам — в частности, к `refreshTenants()` для
детерминированного тестирования жизненного цикла тенантов без ожидания тикера.

| Файл | Что тестирует |
|---|---|
| `config_test.go` | `Config.Validate()` — все поля, комбинации нарушений |
| `executor_registry_test.go` | `ExecutorRegistry` — Register, Get, MustRegister, Keys, конкурентный доступ |
| `manager_test.go` | `WorkerManager` — изоляция тенантов, инварианты Complete, lifecycle |
| `helpers_test.go` | Вспомогательные моки и фабрики, используемые во всех тестах |

### Описание тестов

**Изоляция тенантов** — ключевые тесты для продакшена:

| Тест | Что проверяет |
|---|---|
| `TestTenantConcurrencyLimit` | Семафор блокирует (limit+1)-ю задачу; ровно `WorkerLimit` задач выполняются одновременно |
| `TestTenantIsolation` | Занятый семафор тенанта A не задерживает задачи тенанта B |
| `TestMultipleTenantsConcurrent` | 8 тенантов отправляют задачи параллельно без взаимной блокировки |

**Инвариант Complete:**

| Тест | Что проверяет |
|---|---|
| `TestCompleteCalledExactlyOnce` | `Complete` вызывается ровно 1 раз при успехе, ошибке и панике executor'а |

**Устойчивость пула:**

| Тест | Что проверяет |
|---|---|
| `TestPanicDoesNotKillPool` | После паники в `Execute` воркер перезапускается; пул продолжает работу |
| `TestStopDoesNotDeadlock` | `Stop()` возвращается после `GracefulTimeout` даже при заблокированных задачах |
| `TestStopIdempotent` | Повторный `Stop()` безопасен и не блокирует |

**Lifecycle тенантов:**

| Тест | Что проверяет |
|---|---|
| `TestRefreshTenantsAddTenant` | После добавления тенанта через провайдер `SubmitTask` начинает принимать задачи |
| `TestRefreshTenantsRemoveTenant` | После удаления тенанта `SubmitTask` возвращает ошибку |
| `TestRefreshTenantsUpdateLimit` | После увеличения `WorkerLimit` новое число задач выполняется одновременно |

**Прочие инварианты:**

| Тест | Что проверяет |
|---|---|
| `TestTaskRetryOnError` | `RetryPolicy.Attempts.Count` — точное число повторных попыток |
| `TestContextCancellationPropagated` | Отмена `Task.Ctx` доходит до `Executor.Execute` |
| `TestTenantQueueFull` | `SubmitTask` немедленно возвращает ошибку при заполненном буфере тенанта |
| `TestConfigValidate` | Все поля конфига, объединение нескольких ошибок через `errors.Join` |
| `TestExecutorRegistry_ConcurrentAccess` | Параллельные `Register`/`Get` без гонок |

### Запуск тестов

Базовый запуск:

```bash
go test ./...
```

С детектором гонок (обязательно перед мержем):

```bash
go test -race ./...
```

Конкретный тест:

```bash
go test -race -run TestTenantIsolation ./...
```

Группа тестов по префиксу:

```bash
go test -race -run TestTenant ./...
go test -race -run TestRefreshTenants ./...
```

Подробный вывод:

```bash
go test -race -v ./...
```

С увеличенным таймаутом (для медленных окружений):

```bash
go test -race -timeout 120s ./...
```

### Замечания по тестовой инфраструктуре

- **Нет реального Postgres.** Тесты не требуют внешних зависимостей: `TenantProvider`
  заменён `mockTenantProvider`, `TaskExecutor` — `mockExecutor`.
- **Нет `time.Sleep` как синхронизации**, кроме `TestTenantQueueFull` (30 мс),
  где нужно дать диспетчеру время заблокироваться на `sem.Acquire`.
  Все остальные тесты используют каналы и `sync.WaitGroup`.
- **OTel.** Глобальные провайдеры не инициализируются в тестах — используется
  noop-реализация из `go.opentelemetry.io/otel`, которая не возвращает ошибок
  и не требует настройки.
- **`refreshTenants()` вызывается напрямую** в тестах lifecycle, минуя тикер
  `TenantRefreshInterval`. Это делает тесты детерминированными и быстрыми.

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
