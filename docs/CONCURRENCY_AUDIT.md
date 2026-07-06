# Аудит конкурентности (concurrency) — workerpool

**Skill:** ручное расследование флейка `TestRefreshTenantsUpdateLimit`, обнаруженного при верификации фиксов `docs/SECURITY_AUDIT.md`
**Дата:** 2026-07-06
**Область:** `manager.go` (`dispatch`, `setWorkerCount`, `tenantState`)

## Методология

Находка обнаружена не через grep/статический анализ, а через расследование нестабильного теста: `TestRefreshTenantsUpdateLimit` изредка не проходил (падал на `t.Fatal` после 5 секунд ожидания). Воспроизведено эмпирически на коммите `8e173e4` (до фикса `drainTaskQueue`, т.е. независимо от него) через `git stash` + 3 изолированных прогона теста: 2 прошли успешно, 1 завис. Причина установлена построчным прослеживанием `dispatch()`/`setWorkerCount()` и подтверждена соответствием с фактическим поведением `golang.org/x/sync/semaphore.Weighted.Acquire` и небуферизованной семантикой `select` в Go.

## Findings

### 1. Диспетчер уходящего поколения может «украсть» задачу у нового поколения при смене `WorkerLimit` ✅ исправлено

**Категория:** concurrency-race / generation-handoff · **Severity:** низкая-средняя (не приводит к утечке — `Task.Complete` вызывается корректно, — но временно занижает эффективный лимит конкурентности тенанта в момент смены `WorkerLimit`)

При изменении `WorkerLimit` тенанта `setWorkerCount` (`manager.go:400`) отменяет `genCancel` текущего поколения и немедленно поднимает диспетчер нового поколения с новым семафором — **не дожидаясь**, пока старый диспетчер заметит отмену и завершится:

```go
func (w *WorkerManager) setWorkerCount(state *tenantState, limit int) {
    if state.limit == limit && state.genCancel != nil {
        return
    }

    if state.genCancel != nil {
        state.genCancel()
    }

    genCtx, genCancel := context.WithCancel(state.ctx)
    sem := semaphore.NewWeighted(int64(limit))

    state.sem = sem
    state.genCancel = genCancel
    state.limit = limit

    w.wg.Add(1)
    go w.dispatch(genCtx, state, sem)
}
```

Оба диспетчера — старый (уже отменённый) и новый — читают из **одного и того же** `state.taskQueue`:

```go
// dispatch, шаг 1
select {
case <-genCtx.Done():
    return
case task = <-state.taskQueue:
}
```

`select` в Go недетерминирован между готовыми кейсами. Если старый диспетчер ещё не успел быть перепланирован рантаймом ровно в момент, когда `genCtx.Done()` уже закрыт, а в `taskQueue` уже появилась задача (типичная ситуация при вызове `refreshTenants()` и последующей немедленной постановке задач без промежуточного планирования), `select` может выбрать чтение из `taskQueue` вместо выхода. Задача уходит на `sem.Acquire(genCtx, 1)` со **старым**, уже отменённым `genCtx` и **старой** (обычно меньшей) ёмкостью семафора; `Acquire` почти сразу возвращает ошибку отмены, и задача получает `Complete(ErrDispatcherStopped)` вместо того, чтобы быть переданной новому диспетчеру и реально выполниться в рамках нового, увеличенного лимита.

Полезной нагрузки утечки не происходит (`Complete` всегда вызывается), но это нарушает предположение вызывающего кода, что после `setWorkerCount`/`refreshTenants` новый `WorkerLimit` немедленно и без потерь задач вступает в силу — задача из партии, отправленной сразу после смены лимита, может быть беспричинно отклонена с `ErrDispatcherStopped`, хотя ни тенант, ни диспетчер фактически не останавливались.

Дополнительно: докстринг `tenantState` (`manager.go:64-67`) явно утверждает обратное — «конфликтов между поколениями нет» — что и делало эту находку неочевидной при чтении кода.

**Исправление:** `setWorkerCount` теперь синхронно дожидается полного завершения диспетчера предыдущего поколения (через новый канал `tenantState.genDone`, закрываемый после возврата из `dispatch`), прежде чем поднять диспетчер нового поколения:

```go
if state.genCancel != nil {
    state.genCancel()
    <-state.genDone // дождаться выхода диспетчера предыдущего поколения
}

genCtx, genCancel := context.WithCancel(state.ctx)
sem := semaphore.NewWeighted(int64(limit))
done := make(chan struct{})

state.sem = sem
state.genCancel = genCancel
state.genDone = done
state.limit = limit

w.wg.Add(1)

go func() {
    defer close(done)
    w.dispatch(genCtx, state, sem)
}()
```

Это устраняет саму возможность гонки: в любой момент времени из `state.taskQueue` читает не более одного диспетчера. Ожидание безопасно и ограничено по времени — `dispatch` не имеет ни одного блокирующего вызова, не учитывающего `genCtx` (`select` шага 1 и `sem.Acquire` шага 2 оба реагируют на отмену немедленно; `pool.addTask` шага 4 неблокирующий — использует `select`/`default`), поэтому диспетчер предыдущего поколения завершается практически сразу после `genCancel()`, без риска долгой блокировки `tenantsMu`.

Докстринг `tenantState` поправлен: снято неверное утверждение об отсутствии конфликтов между поколениями, взамен описан новый инвариант (не более одного активного диспетчера на тенант одновременно).

Покрыто тестом `TestSetWorkerCountNoGenerationOverlap` (`manager_test.go`), который эмпирически воспроизводит сценарий находки: тенант с малым лимитом, задачи занимают все слоты, лимит увеличивается, сразу вслед за этим отправляется пачка новых задач — все они должны быть приняты новым поколением и выполниться конкурентно согласно новому лимиту, ни одна не должна получить `ErrDispatcherStopped`.

### 2. `SubmitTask` может отправить задачу в `taskQueue` уже удалённого тенанта — задача теряется навсегда (`Complete` никогда не вызывается) ✅ исправлено

**Категория:** concurrency-race / TOCTOU · **Severity:** средняя — в отличие от находки №1, здесь происходит настоящая утечка: `Task.Complete` не вызывается вообще, и вызывающий код (например, River) никогда не узнаёт, что задача не выполнилась.

**Обнаружено:** методический разбор всех `select`-блоков и путей блокировок в рамках аудита `/cc-skills-golang:golang-concurrency`, эмпирически подтверждено стресс-тестом (см. ниже).

До исправления `SubmitTask` (`manager.go:239`) отпускал `tenantsMu` сразу после поиска `state`, ещё до отправки задачи в очередь:

```go
w.tenantsMu.RLock()
state, ok := w.tenants[tenantID]
w.tenantsMu.RUnlock()

if !ok {
    return fmt.Errorf("%w: tenant %s", ErrTenantNotFound, tenantID)
}

select {
case state.taskQueue <- task:
    return nil
case <-state.ctx.Done():
    return fmt.Errorf("%w: tenant %s", ErrTenantShuttingDown, tenantID)
default:
    return fmt.Errorf("%w: tenant %s, capacity %d", ErrTenantQueueFull, tenantID, cap(state.taskQueue))
}
```

Между `RUnlock()` и `select` есть окно, в течение которого `refreshTenants()` (ветка удаления тенанта, `manager.go:342-354`) может под эксклюзивным `Lock()` полностью выполнить `state.cancel()` → `drainTaskQueue(state)` → `delete(w.tenants, id)`. Горутина `SubmitTask` при этом продолжает держать **устаревший, но валидный** указатель на `state` (Go не инвалидирует память при удалении из map). Когда она наконец доходит до `select`, оба содержательных кейса могут оказаться одновременно готовы: `state.ctx.Done()` уже закрыт (тенант удалён), и `state.taskQueue` уже имеет свободное место (только что опустошён `drainTaskQueue`). `select` в Go недетерминирован между готовыми кейсами — он может выбрать отправку в `taskQueue`, а не выход по `ctx.Done()`. Задача попадает в канал, который к этому моменту:

- не читается диспетчером — тот уже завершился (`genCtx`, производный от `state.ctx`, тоже отменён);
- не будет повторно дренирован — `drainTaskQueue` для этого тенанта уже отработал и не вызывается снова;
- недостижим из `w.tenants` — тенант удалён из map.

Задача зависает в канале до сборки мусора, и `task.Complete` не вызывается никогда — сравните с находкой №1 и с уже исправленным `drainTaskQueue`, где обе стороны гонки сходятся к одинаковому корректному результату (`ErrDispatcherStopped`); здесь же гонка приводит к полной потере колбэка.

**Эмпирическое подтверждение:** временный стресс-тест (тенант с включённым/выключенным присутствием, переключаемым в цикле через `refreshTenants`, конкурентно с 8 горутинами, непрерывно вызывающими `SubmitTask`) воспроизвёл утечку в 2 из 2 прогонов: `submitted=7553 completed=7550` (gap=3) и `submitted=5683 completed=5665` (gap=18).

**Исправление:** `RLock` теперь удерживается на протяжении всего тела `SubmitTask` — от поиска `state` до завершения `select` — вместо освобождения сразу после поиска:

```go
w.tenantsMu.RLock()
defer w.tenantsMu.RUnlock()

state, ok := w.tenants[tenantID]
if !ok {
    return fmt.Errorf("%w: tenant %s", ErrTenantNotFound, tenantID)
}

select {
case state.taskQueue <- task:
    return nil
case <-state.ctx.Done():
    return fmt.Errorf("%w: tenant %s", ErrTenantShuttingDown, tenantID)
default:
    return fmt.Errorf("%w: tenant %s, capacity %d", ErrTenantQueueFull, tenantID, cap(state.taskQueue))
}
```

`refreshTenants` требует эксклюзивный `Lock()`, поэтому пока хотя бы одна горутина `SubmitTask` держит `RLock()`, ветка удаления тенанта не может начаться — окно гонки закрыто полностью. Критическая секция не становится блокирующей: `select` внутри неё неблокирующий (есть `default`), поэтому удержание `RLock()` не рискует надолго задержать конкурентный `refreshTenants`.

После фикса тот же стресс-тест (переиспользован как `TestSubmitTaskNoRaceWithTenantRemoval`, `manager_test.go`) даёт `submitted == completed` в каждом из прогонов (проверено 3 раза подряд).

**Побочный эффект:** `SubmitTask` теперь на время своего (неблокирующего) тела блокирует `refreshTenants` от получения эксклюзивного `Lock`, и наоборот — конкурентный `refreshTenants` может ненадолго задержать `SubmitTask` (`sync.RWMutex` в Go отдаёт приоритет уже ожидающему `Lock()`, чтобы избежать голодания писателя). Ожидание внутри `refreshTenants` тоже ограничено: `setWorkerCount`'s `<-state.genDone` (находка №1) завершается практически сразу, поэтому суммарная задержка остаётся малой.

## Проверено и не вызывает замечаний

- **Направленность каналов** (`chan<-`/`<-chan`) нигде не используется в сигнатурах — не является находкой: все каналы (`taskQueue`, `taskChan`, `genDone`) полностью инкапсулированы внутри пакета и никогда не передаются наружу как параметры функций, где направленность реально ограничивала бы неверное использование.
- **`pool.addTask`** — уже корректно защищён симметричным паттерном: `closeMu.RLock()` удерживается на протяжении проверки `isStopping` и отправки в `taskChan`, что и навело на находку №2 (тот же паттерн отсутствовал в `SubmitTask`).
- **`pool.stop()`** — закрытие `taskChan` под `closeMu.Lock()`, ожидание воркеров через отдельную горутину + `select`/`time.After` (единственное использование `time.After` в кодовой базе, не в горячем цикле — безопасно).
- **`executeWithRetry`** — использует `time.NewTimer`, а не `time.After`, в цикле повторных попыток; таймер останавливается (`timer.Stop()`) при досрочном выходе по `ctx.Done()` — исключает утечку таймера. Полный jitter в `exponentialBackoff` — на месте.
- **`tenantRefresher`** — переиспользует один `time.Ticker` (не пересоздаёт таймер в цикле), `select` включает `w.ctx.Done()`.
- **`WorkerManager.Stop()`** — порядок (`cancel()` → отмена контекстов тенантов под `tenantsMu.Lock()` → `wg.Wait()` → `pool.stop()`) и happens-before между `isStopping`/`wg.Add` уже задокументированы в докстринге и защищают от гонки `wg.Add`/`wg.Wait()` (`go test -race` подтверждает).
- **`wg.Add`/`wg.Done` парность** — во всех местах (`tenantRefresher`, `dispatch`) `Add(1)` вызывается до соответствующего `go`, `Done()` — через `defer` в начале горутины; `pool.wg` использует `wg.Go` (Go 1.25+), что структурно исключает рассинхронизацию.
- **`ExecutorRegistry`** — простой `sync.RWMutex` без вложенных вызовов, поддерживает нулевое значение, замечаний нет.
- **`sync.Map`** — не используется; не является находкой, так как все map-структуры (`w.tenants`, `r.executors`) защищены явным `RWMutex` с чёткими границами критических секций, что для данного кейса читаемее и не создаёт накладных расходов сверх необходимого.
- **`goleak`** — в тестах не используется. Не поднимается как находка (тесты и так проверяют завершение через явные `wg`/каналы и `t.Cleanup(m.Stop)`), но может быть полезным дополнением на будущее для автоматического обнаружения утечек горутин в новых тестах.
