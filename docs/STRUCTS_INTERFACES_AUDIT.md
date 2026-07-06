# Аудит структур и интерфейсов (structs & interfaces) — workerpool

**Skill:** `samber/cc-skills-golang@golang-structs-interfaces`
**Дата:** 2026-07-06
**Область:** весь пакет — `config.go`, `errors.go`, `executor_registry.go`, `health.go`, `manager.go`, `pool.go` (production-код), плюс `helpers_test.go`, `example_test.go` (типы, реализующие публичные интерфейсы пакета)

## Методология

Проверка на соответствие практикам skill'а: размер и место определения интерфейсов, "accept interfaces, return structs", преждевременные интерфейсы, полезность нулевых значений, `any`/`interface{}` вместо дженериков, compile-time проверка реализации интерфейса, безопасность type assertion, embedding, DI через интерфейсы, теги полей структур, консистентность receiver'ов, `noCopy`-сентинел. Инвентаризация всех интерфейсов/структур и их методов через grep, построчное чтение `manager.go`, `pool.go`, `health.go`, `config.go`, `executor_registry.go`, `helpers_test.go`, `example_test.go`.

## Findings

### 1. Отсутствуют compile-time проверки реализации интерфейсов у mock/example-типов ✅ исправлено

**Категория:** missing-interface-check · **Severity:** низкая (стоимость исправления против пользы — тривиальна)

`mockTenant`, `mockTenantProvider`, `mockExecutor` (`helpers_test.go`) и `echoExecutor`, `staticTenant`, `staticProvider` (`example_test.go`) реализуют `Tenant`/`TenantProvider`/`TaskExecutor`, но нигде в пакете не было ни одной проверки вида `var _ Interface = (*Type)(nil)`.

Это не только отклонение от рекомендации skill'а — в этой же сессии несколько раз возникала путаница из-за stale-диагностики IDE ("`*mockTenant` does not implement `Tenant` (missing method `ID`)"), которую приходилось каждый раз перепроверять вручную через `go build`/`go vet`, хотя метод `ID` фактически присутствует. Явная compile-time проверка рядом с определением типа не стоит ничего в рантайме и делает несоответствие интерфейсу видимым сразу в месте объявления, а не косвенно — через ошибку в тесте или недостоверную IDE-подсказку.

**Исправление:** добавлены проверки рядом с каждым определением типа:

```go
// helpers_test.go
var _ Tenant = (*mockTenant)(nil)
var _ TenantProvider = (*mockTenantProvider)(nil)
var _ TaskExecutor = (*mockExecutor)(nil)
```

```go
// example_test.go (внешний тестовый пакет workerpool_test)
var _ workerpool.TaskExecutor = echoExecutor{}
var _ workerpool.Tenant = staticTenant{}
var _ workerpool.TenantProvider = staticProvider{}
```

Подтверждено `go build ./...`/`go vet ./...`/`golangci-lint run ./...` — без замечаний.

### 2. `HealthStatus`/`TenantHealth` — экспортируемые поля без тегов сериализации, хотя докстринг сам анонсирует HTTP-экспозицию ✅ исправлено

**Категория:** missing-field-tags · **Severity:** низкая

Докстринг `HealthStatus` (см. `docs/SECURITY_AUDIT.md`, находка №3) прямо описывает сценарий, в котором снимок транслируется во внешний `/health`/`/ready` HTTP-эндпоинт — но ни одно поле `HealthStatus`/`TenantHealth` не имело тега `json:"..."`. Без тега сериализация по умолчанию отдаёт поля в исходном Go-регистре (`TenantID`, `PoolQueueDepth`), а не в принятом для HTTP JSON API `snake_case`; кроме того, любое переименование Go-поля незаметно меняет формат ответа для внешних потребителей, поскольку имя JSON-ключа неявно привязано к имени поля.

**Исправление:** обоим типам (`health.go`) добавлены явные `json`-теги в `snake_case`:

```go
type HealthStatus struct {
    Healthy           bool           `json:"healthy"`
    Stopping          bool           `json:"stopping"`
    PoolQueueDepth    int            `json:"pool_queue_depth"`
    PoolQueueCapacity int            `json:"pool_queue_capacity"`
    PoolWorkerCount   int            `json:"pool_worker_count"`
    TenantCount       int            `json:"tenant_count"`
    Tenants           []TenantHealth `json:"tenants"`
}

type TenantHealth struct {
    TenantID      uuid.UUID `json:"tenant_id"`
    QueueDepth    int       `json:"queue_depth"`
    QueueCapacity int       `json:"queue_capacity"`
    WorkerLimit   int       `json:"worker_limit"`
}
```

Логика `Health()` не менялась. Подтверждено `go build ./...`/`go vet ./...`/`golangci-lint run ./...` — без замечаний.

## Без замечаний

Проверено и соответствует правилам skill'а без отклонений:

- **Размер и место определения интерфейсов** — `TenantProvider` (1 метод), `Tenant` (2 метода), `TaskExecutor` (1 метод) все определены в `manager.go`, на стороне потребителя (`WorkerManager`), а не в гипотетическом пакете-реализации. Ни один интерфейс не превышает 3 методов.
- **"Accept interfaces, return structs"** — `NewWorkerManager(WorkerManagerParams) (*WorkerManager, error)` и `NewExecutorRegistry() *ExecutorRegistry` возвращают конкретные типы; `WorkerManagerParams.TenantProvider` типизировано как интерфейс `TenantProvider`, а не как конкретная реализация. `newPool(poolParams) (*pool, error)` — аналогично.
- **Преждевременные интерфейсы** — `TenantProvider`/`Tenant`/`TaskExecutor` являются точками расширения библиотеки, предназначенными для внешних реализаций потребителями пакета; это не преждевременная абстракция с единственной реализацией «на всякий случай», а осознанная граница библиотеки.
- **Полезность нулевых значений** — `ExecutorRegistry{}` безопасен без конструктора (см. `docs/SAFETY_AUDIT.md`, находка №1, уже устранена ленивой инициализацией карты). `Config{}` — чистый value-тип, невалидные нулевые значения отлавливаются `Validate()`.
- **`any`/`interface{}`** — grep по production-файлам не выявил ни одного использования `any`/`interface{}` в объявлениях функций/структур; единственное использование `any` — `slog.Any("panic", r)` в `pool.go`, это сигнатура стандартной библиотеки (`log/slog`), а не дизайнерское решение пакета.
- **Безопасность type assertion** — bare type assertions отсутствуют во всём production-коде (уже подтверждено в `docs/SAFETY_AUDIT.md`).
- **Embedding** — struct/interface embedding в пакете не используется; все зависимости оформлены именованными полями (`WorkerManager.provider TenantProvider`, `WorkerManager.pool *pool`), что корректно для пакета, где нет цели «продвигать» весь API вложенного типа наружу.
- **DI через интерфейсы** — `WorkerManagerParams.TenantProvider` и разрешение `Task.Executor`/`ExecutorKey` через `*ExecutorRegistry` — оба пути внедрения зависимостей оформлены через интерфейсы (`TenantProvider`, `TaskExecutor`), а не через конкретные типы.
- **Теги полей структур** — `Config` и вложенные `RetryPolicy`/`JitterConfig`/`AttemptsConfig` полностью покрыты тегами `yaml:"..."` и `env-default:"..."` для всех экспортируемых полей — соответствует своей роли конфигурации, загружаемой out-of-band (не JSON API).
- **Консистентность receiver'ов** — `Config.Validate()` — единственный метод на `Config`, value receiver корректен для чистого value-типа без мьютексов/срезов/карт. `WorkerManager`, `pool`, `ExecutorRegistry` (все содержат `sync.Mutex`/`sync.RWMutex`) — все методы на pointer receiver, без единого исключения. `staticTenant`/`staticProvider`/`echoExecutor` (`example_test.go`) — все методы на value receiver, консистентно для простых иммутабельных demo-типов. `mockTenant`/`mockExecutor`/`mockTenantProvider` (`helpers_test.go`) — все методы на pointer receiver.
- **`noCopy`-сентинел** — `WorkerManager` и `pool` содержат `sync.RWMutex`/`sync.WaitGroup` напрямую как поля; `go vet`'s встроенный анализ `copylocks` уже защищает от копирования значений этих типов без необходимости в дополнительном ручном `noCopy`-сентинеле. Оба типа, кроме того, конструируются исключительно через `New*`/`new*`, возвращающие указатель, что делает случайное копирование по значению маловероятным даже без учёта `copylocks`.
- **Каноничные имена методов стандартных интерфейсов** — в пакете нет типов, претендующих на `io.Reader`/`fmt.Stringer`/`json.Marshaler` и т.п., поэтому вопрос соответствия каноничным сигнатурам неприменим.
