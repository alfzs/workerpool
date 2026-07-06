# Аудит обработки ошибок — workerpool

**Skill:** `samber/cc-skills-golang@golang-error-handling`
**Дата:** 2026-07-06
**Область:** `config.go`, `doc.go`, `executor_registry.go`, `health.go`, `manager.go`, `pool.go` (production-код, без тестов, ~830 строк)

## Методология

Проверка на соответствие 15 практикам skill'а: проверка возвращаемых ошибок, wrapping (`%w`/`%v`), формат сообщений, `errors.Is`/`errors.As`/`errors.Join`, правило единственной обработки (log OR return), `panic`/`recover`, структурированное логирование через `slog`, кардинальность лог-сообщений.

## Findings

### 1. Двойное логирование при отказе задачи — нарушение правила единственной обработки — ✅ исправлено

**Категория:** single-handling-rule · **Severity:** основное

Было (`pool.go`, `executeWithRetry` + `runTask`): ошибка логировалась на уровне Error **и одновременно** возвращалась как `lastErr`, а затем через `runTask` попадала в `task.Complete(taskErr)`. Пример использования из `doc.go:65-70` сам демонстрировал антипаттерн — при следовании документированному способу каждый отказ задачи попадал в лог дважды: один раз из библиотеки, один раз из кода вызывающей стороны. Паттерн повторялся ещё дважды: при панике задачи (`pool.go`, `runTask`) и при отказе пула по backpressure (`manager.go`, `dispatch`).

**Исправление:** владение логированием закреплено за одной стороной по правилу «лог — только запасной канал, если обработать ошибку больше некому»:

```go
// pool.go — runTask
if task.Complete != nil {
    task.Complete(taskErr)
    return
}
if taskErr != nil {
    p.logger.Error("task failed with no completion handler", ...)
}
```

Дублирующий лог `"task failed after all retries"` в `executeWithRetry` удалён — решение о логировании принимает только `runTask`, которому известно, есть ли получатель `Complete`. Та же логика применена в `manager.go` (`dispatch`): Warn `"pool rejected task with no completion handler"` печатается только если у задачи нет исходного `Complete`-получателя (`original == nil`), иначе ошибка молча передаётся вызывающему коду через `Complete`.

Исключение оставлено осознанно: лог паники (`"task panic"`, со стек-трейсом) логируется безусловно, даже если `Complete` задан — стек-трейс несёт информацию о дефекте в `Executor`, которой нет в возвращаемой ошибке, и не дублирует то, что получит вызывающий код через `Complete`.

### 2. Отсутствуют sentinel/типизированные ошибки для ожидаемых условий — ✅ исправлено

**Категория:** sentinel-errors · **Severity:** основное

Было: `SubmitTask`, `addTask` и `ExecutorRegistry.Get`/`Register` возвращали только `fmt.Errorf(...)` для явно различимых и ожидаемых условий («очередь заполнена» — временный backpressure против «тенант не найден» — постоянное условие и т. д.), без единой sentinel-ошибки в пакете и без единого вызова `errors.Is`/`errors.As`.

**Исправление:** добавлен `errors.go` с sentinel-ошибками, проверяемыми через `errors.Is`:

```go
var (
    ErrPoolStopping = errors.New("pool is stopping")
    ErrQueueFull    = errors.New("pool queue full")
)
var (
    ErrTenantNotFound     = errors.New("tenant not found")
    ErrTenantShuttingDown = errors.New("tenant is shutting down")
    ErrTenantQueueFull    = errors.New("tenant queue full")
    ErrDispatcherStopped  = errors.New("dispatcher stopped before task could run")
)
var (
    ErrEmptyExecutorKey          = errors.New("executor key cannot be empty")
    ErrNilExecutor               = errors.New("executor cannot be nil")
    ErrExecutorAlreadyRegistered = errors.New("executor already registered")
    ErrExecutorNotFound          = errors.New("executor not found")
)
```

Точки возврата в `pool.go` (`addTask`), `manager.go` (`SubmitTask`, `dispatch`) и `executor_registry.go` (`Register`, `Get`) переведены на `fmt.Errorf("%w: <контекст>", ErrX, ...)`, где есть динамические данные (ID тенанта, ёмкость очереди, ключ executor'а), и на прямой возврат sentinel-а, где контекста нет (`ErrPoolStopping`, `ErrEmptyExecutorKey`). Как побочный эффект, ушли три предупреждения `perfsprint` (`fmt.Errorf` без аргументов подстановки), уже отмеченные golangci-lint.

### 3. Ошибка провайдера тенантов возвращается без контекста — ✅ исправлено

**Категория:** error-wrapping-minor · **Severity:** низкая

Было: `refreshTenants` возвращала сырую ошибку `w.provider.List(ctx)` без обёртки, из-за чего в периодическом логе `tenantRefresher` терялся контекст «это ошибка получения списка тенантов».

**Исправление:**

```go
tenants, err := w.provider.List(w.ctx)
if err != nil {
    return fmt.Errorf("list tenants: %w", err)
}
```

## Без замечаний

Соответствует правилам skill'а без отклонений:

- Нет отброшенных ошибок (`_ = ...`) — проверено grep'ом по всему production-коду.
- `%w` используется последовательно для внутреннего wrapping; `%v` — только для значения паники (`any`, не `error`), где `%w` неприменим.
- Сообщения об ошибках в нижнем регистре, без завершающей пунктуации (заглавные буквы — только в именах экспортируемых полей конфига, что является общепринятым исключением).
- `errors.Join` использован корректно в `Config.Validate` для агрегации независимых ошибок валидации.
- `panic` встречается только в `MustRegister`, зарезервирован для программных ошибок инициализации (`init`/`TestMain`) — не используется для ожидаемых условий.
- `slog` используется единообразно везде, уровни логирования (Error/Warn/Info/Debug) выбраны уместно.
- Лог-сообщения — стабильные низкокардинальные шаблоны, ID и счётчики передаются как структурированные атрибуты, а не встроены в текст сообщения.
