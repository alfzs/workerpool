# Аудит безопасности кода (safety) — workerpool

**Skill:** `samber/cc-skills-golang@golang-safety`
**Дата:** 2026-07-06
**Область:** `config.go`, `doc.go`, `errors.go`, `executor_registry.go`, `health.go`, `manager.go`, `pool.go` (production-код, без тестов, ~830 строк)

## Методология

Проверка на соответствие 11 практикам skill'а: nil-ловушки (typed nil в интерфейсе, nil map/slice/channel), безопасность срезов и карт (aliasing через `append`), численная безопасность (усечение при конвертации, деление на ноль, сравнение float), безопасность ресурсов (`defer` в цикле, утечки горутин), защитное копирование в экспортируемых функциях, проектирование нулевых значений, `sync.Once` для ленивой инициализации, безопасные type assertion.

## Findings

### 1. `ExecutorRegistry{}` без конструктора паникует на первом `Register` — nil map write ✅ исправлено

**Категория:** zero-value-design / nil-map · **Severity:** высокая

`ExecutorRegistry.executors` инициализируется только в `NewExecutorRegistry()`:

```go
type ExecutorRegistry struct {
    mu        sync.RWMutex
    executors map[string]TaskExecutor
}

func NewExecutorRegistry() *ExecutorRegistry {
    return &ExecutorRegistry{executors: make(map[string]TaskExecutor)}
}
```

`Get` и `Keys` безопасны на nil-карте — чтение, `range` и `len` нулевой карты не паникуют. Но `Register` не делает ленивую инициализацию:

```go
func (r *ExecutorRegistry) Register(key string, exec TaskExecutor) error {
    ...
    r.mu.Lock()
    defer r.mu.Unlock()
    if _, exists := r.executors[key]; exists { ... }
    r.executors[key] = exec // panic: assignment to entry in nil map, если executors == nil
    return nil
}
```

Если вызывающий код объявит `var r workerpool.ExecutorRegistry` (или встроит `ExecutorRegistry` в свою структуру по значению) вместо вызова `NewExecutorRegistry()`, первый же `Register`/`MustRegister` паникует. В отличие от `Config`, для которого невалидный нулевое значение отлавливается штатным `Validate()`, здесь нет никакого предохранителя и нет предупреждения в докстринге, аналогичного `Config`'овскому "нулевые значения недопустимы".

**Исправление:** ленивая инициализация карты под `r.mu.Lock()` в начале `Register` (`executor_registry.go`); докстринг структуры дополнен явным указанием, что нулевое значение готово к использованию:

```go
r.mu.Lock()
defer r.mu.Unlock()

if r.executors == nil {
    r.executors = make(map[string]TaskExecutor)
}
```

Покрыто тестом `TestExecutorRegistry_Register/zero_value` (`executor_registry_test.go`).

### 2. `Task.Ctx == nil` / `Task.Executor == nil` падают паникой глубоко в воркере вместо понятной ошибки в `SubmitTask` ✅ исправлено

**Категория:** nil-safety / defensive-validation-at-boundary · **Severity:** средняя

`WorkerManager.SubmitTask` не проверяет обязательные поля `Task` перед постановкой в очередь тенанта:

```go
func (w *WorkerManager) SubmitTask(tenantID uuid.UUID, task Task) error {
    ...
    select {
    case state.taskQueue <- task:
        return nil
    ...
    }
}
```

Дальше задача уходит в `dispatch` → `pool.addTask` → `pool.runTask` → `executeWithRetry`, где:

- `context.WithCancel(task.Ctx)` паникует, если `task.Ctx == nil` (`context.WithCancel` на Go 1.26 паникует на nil-контексте);
- `task.Executor.Execute(ctx, ...)` паникует с nil pointer dereference, если `task.Executor == nil` (вызов метода на nil-интерфейсе).

Обе паники перехватываются `recover()` в `runTask` (см. `docs/ERROR_HANDLING_AUDIT.md`, находка №1) — процесс не падает, ошибка доходит до `Complete`/лога. Но:

- ошибка выглядит как `panic: context: Cancel called on a nil Context` / `panic: runtime error: invalid memory address or nil pointer dereference` — не как понятная sentinel-ошибка;
- она обнаруживается только когда задача добралась до воркера, а не сразу в `SubmitTask`, где вызывающий код мог бы отреагировать синхронно.

**Исправление:** явная проверка в начале `SubmitTask` (`manager.go`), с новыми sentinel-ошибками `ErrTaskNilContext` и `ErrTaskNoExecutor` в `errors.go`:

```go
if task.Ctx == nil {
    return ErrTaskNilContext
}

if task.Executor == nil {
    if task.ExecutorKey == "" {
        return ErrTaskNoExecutor
    }
    // ... см. находку №3 — резолвинг ExecutorKey перед постановкой в очередь
}
```

Покрыто тестами `TestSubmitTaskNilContext`, `TestSubmitTaskNoExecutor` (`manager_test.go`).

### 3. `Task.ExecutorKey` документирован, но нигде не читается ✅ исправлено (вариант a)

**Категория:** misleading-api / nil-safety-adjacent · **Severity:** средняя

Докстринг поля:

```go
// ExecutorKey — строковый ключ для разрешения executor'а через
// ExecutorRegistry. Заполняется, если executor хранится в реестре
// (типичный случай при использовании River как job store).
ExecutorKey string
```

подразумевает автоматическое разрешение через `ExecutorRegistry`. Но ни `pool.go`, ни `manager.go` нигде не хранят ссылку на `*ExecutorRegistry` и не вызывают `registry.Get(task.ExecutorKey)` — `executeWithRetry` безусловно использует только `task.Executor`:

```go
err := task.Executor.Execute(ctx, task.TenantID, workerID)
```

Подтверждено `grep`: `ExecutorKey` встречается только в объявлении поля, его докстринге и упоминании в `doc.go` — нигде как значение, которое читается пулом. И README, и `doc.go`, и `example_test.go` фактически используют паттерн ручного резолвинга (`exec, _ := registry.Get("sync_orders"); ... Executor: exec`), то есть сам пакет никогда не полагается на автоматическое разрешение — но докстринг поля обещает обратное.

Если вызывающий код доверится докстрингу буквально и заполнит только `ExecutorKey`, оставив `Executor` нулевым, — получит находку №2 (паника на nil-интерфейсе) при каждом запуске такой задачи.

**Исправление (вариант a):** в `WorkerManagerParams` добавлено опциональное поле `ExecutorRegistry *ExecutorRegistry`; `WorkerManager` хранит его в `executorRegistry` и использует в `SubmitTask` для резолвинга `ExecutorKey → Executor`, когда `Executor == nil`:

```go
if task.Executor == nil {
    if task.ExecutorKey == "" {
        return ErrTaskNoExecutor
    }

    if w.executorRegistry == nil {
        return fmt.Errorf("%w: key %q", ErrNoExecutorRegistry, task.ExecutorKey)
    }

    exec, err := w.executorRegistry.Get(task.ExecutorKey)
    if err != nil {
        return fmt.Errorf("resolve executor: %w", err)
    }

    task.Executor = exec
}
```

Резолвинг происходит синхронно в `SubmitTask`, до постановки в очередь тенанта — задача либо уходит в очередь с уже разрешённым `Executor`, либо `SubmitTask` немедленно возвращает понятную ошибку (`ErrNoExecutorRegistry`, если реестр не сконфигурирован, или обёрнутую `ErrExecutorNotFound`, если ключ не зарегистрирован). Если ни `Executor`, ни `ExecutorKey` не заданы — `ErrTaskNoExecutor` (находка №2).

Новая sentinel-ошибка `ErrNoExecutorRegistry` в `errors.go`. Покрыто тестами `TestSubmitTaskExecutorKeyWithoutRegistry`, `TestSubmitTaskExecutorKeyUnknown`, `TestSubmitTaskExecutorKeyResolved` (`manager_test.go`).

### 4. Typed-nil ловушка в `ExecutorRegistry.Register`'s проверке `exec == nil`

**Категория:** nil-interface-trap · **Severity:** низкая (пограничная, стоимость исправления против вероятности)

```go
func (r *ExecutorRegistry) Register(key string, exec TaskExecutor) error {
    ...
    if exec == nil {
        return fmt.Errorf("%w: key %q", ErrNilExecutor, key)
    }
    ...
}
```

Если вызывающий код передаёт типизированный nil-указатель, реализующий `TaskExecutor` (`var e *MyExecutor; registry.Register("x", e)`), интерфейс `exec` хранит `(тип=*MyExecutor, значение=nil)` — сравнение `exec == nil` возвращает `false`, проверка молча пропускает нулевой executor. При последующем вызове `exec.Execute(...)` метод получит nil-receiver — большинство реализаций запаникуют при разыменовании внутренних полей.

Классическая и хорошо документированная ловушка Go. Полное исправление требует `reflect.ValueOf(exec).IsNil()` с предварительной проверкой `Kind()` (Ptr/Interface/Map/Chan/Func/Slice, иначе `IsNil()` сама паникует) — заметное усложнение простой функции ради редкой ошибки вызывающего кода. Оставлено как задокументированное ограничение, не как обязательное к исправлению.

## Без замечаний

Проверено и соответствует правилам skill'а без отклонений:

- Нет ни одного «голого» type assertion (`x.(T)` без `, ok`) во всём production-коде — проверено grep'ом.
- Нет усечения при конвертации целых типов; единственная конвертация (`int64(limit)` в `setWorkerCount`) расширяющая (int → int64), не сужающая, и `limit` уже гарантированно `> 0` к этому моменту (`refreshTenants` подставляет 1 при `limit <= 0`).
- `rand.N(exp)` в `exponentialBackoff` вызывается только после явной проверки `exp <= 0` — `rand.N` паникует на неположительном аргументе, но путь исполнения этого не допускает.
- Нет сравнения float через `==` — арифметика с плавающей точкой в пакете не используется (только `time.Duration`, целочисленный тип).
- Нет `defer` внутри циклов — единственный цикл, порождающий горутины (`pool.start()`), использует `p.wg.Go(func() { p.runWorker(workerID) })` без `defer` в теле цикла; замыкание над `workerID` безопасно благодаря per-iteration семантике цикловых переменных в Go 1.22+ (`go.mod`: `go 1.26.0`).
- Все экспортируемые методы, возвращающие срезы (`Keys()`, `GetTenantIDs()`, `Health().Tenants`), строят новый срез через `make`+`append`/`range`, не берут подсрез от внутреннего состояния — aliasing через `append` невозможен, защитное копирование не требуется дополнительно.
- `Config` и вложенные `RetryPolicy`/`JitterConfig`/`AttemptsConfig` — чистые value-типы без срезов/карт/указателей, поэтому передача `Config` по значению в `pool`/`WorkerManager` не создаёт скрытого совместного владения.
- `sync.Once` в пакете не требуется: единственная лениво-инициализируемая globalная зависимость — OTel-провайдеры — уже реализована как потокобезопасный `otel.GetTracerProvider()`/`otel.GetMeterProvider()` на стороне библиотеки `go.opentelemetry.io/otel`.
