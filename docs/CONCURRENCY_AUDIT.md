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
