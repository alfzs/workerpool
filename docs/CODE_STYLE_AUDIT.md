# Аудит стиля кода — workerpool

**Skill:** `samber/cc-skills-golang@golang-code-style`
**Дата:** 2026-07-06
**Область:** `config.go`, `doc.go`, `executor_registry.go`, `health.go`, `manager.go`, `pool.go` (production-код, без тестов, ~830 строк)

## Методология

Проверка на соответствие правилам skill'а: длина строк, `var`/`:=`, инициализация срезов/карт, композитные литералы, control flow (`else`/`switch`), количество и порядок параметров функций, `range`-циклы, обработка строк, организация файлов.

## Findings

### 1. Лишний `else` после `return` — `pool.go:276-281` — ✅ исправлено

**Категория:** style-else · **Severity:** основное

Было:

```go
if err := task.Executor.Execute(ctx, task.TenantID, workerID); err == nil {
    p.recordCompletion(task, time.Since(start), "success")
    return nil
} else {
    lastErr = err
}
```

Нарушало правило "Eliminate Unnecessary else": если `if`-ветка заканчивается `return`, `else` должен быть убран.

Стало (`err` вынесен из условия `if`, так как область видимости переменной, объявленной в `if`, не распространяется за пределы всей конструкции `if/else`):

```go
err := task.Executor.Execute(ctx, task.TenantID, workerID)
if err == nil {
    p.recordCompletion(task, time.Since(start), "success")
    return nil
}
lastErr = err
```

### 2. Порядок параметров: `context.Context` не первым — `manager.go:360` — ✅ исправлено

**Категория:** style-param-order · **Severity:** основное

Было:

```go
func (w *WorkerManager) dispatch(state *tenantState, sem *semaphore.Weighted, genCtx context.Context) {
```

Правило: `context.Context` первым, затем входы, затем выходные параметры. `genCtx` стоял последним.

Стало (сигнатура и место вызова в `setWorkerCount` обновлены):

```go
func (w *WorkerManager) dispatch(genCtx context.Context, state *tenantState, sem *semaphore.Weighted) {
```

### 3. `var errs []error` — nil-срез — `config.go:85`

**Категория:** style-nil-slice-minor · **Severity:** низкая (информационно)

```go
var errs []error
```

Формально нарушает правило "slices/maps MUST be initialized explicitly, never nil", но это общепринятая Go-идиома для error-аккумулятора: `append` на nil-срезе безопасен, `errors.Join` принимает nil, срез не сериализуется в JSON и не возвращается наружу как API-значение. Обоснование правила (паника при записи в nil map, `null` вместо `[]` в JSON) здесь неприменимо — менять не рекомендуется.

### 4. Индексный цикл вместо `range` — `pool.go:332` — ✅ исправлено

**Категория:** style-range-loop-minor · **Severity:** низкая

Было:

```go
exp := minDelay
for i := 1; i < attempt; i++ {
    ...
}
```

`i` использовался только как счётчик итераций, значение не читалось. По правилу "Use range n (Go 1.22+) for simple counting" цикл переписан на `for range attempt - 1 { ... }` — согласуется с уже применённым в проекте паттерном (`pool.go:140`: `for workerID := range p.workerCount`).

## Без замечаний

Соответствует правилам skill'а без отклонений:

- Все функции ≤4 параметров, ни одна не требует options-struct.
- Композитные литералы везде используют имена полей.
- Срезы и карты (кроме п. 3) инициализируются через `make`/литерал, не nil.
- Остальные `if/else` в пакете (`manager.go:285-289`) корректны — обе ветки не заканчиваются `return`/`break`/`continue`.
- Нет цепочек `if-else` по одной переменной, которые стоило бы заменить на `switch`.
- Нет длинных строк (>120 символов), нет dot-imports, нет blank-imports вне `main`/тестов.
- Строковые сообщения об ошибках используют `%q` для ключей (`executor_registry.go`) и `%d`/`%s` уместно.
- Организация файлов соответствует порядку: doc → imports → types → constructor → methods → helpers.
