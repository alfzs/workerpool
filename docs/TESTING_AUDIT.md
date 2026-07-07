# Аудит тестирования (testing) — workerpool

**Skill:** `samber/cc-skills-golang@golang-testing` (audit mode, 3 параллельных саб-агента: качество unit-тестов и покрытие, изоляция интеграционных тестов, утечки горутин/гонки)
**Дата:** 2026-07-07
**Область:** `*_test.go` (весь пакет `workerpool`, ~1500 строк тестов + бенчмарков)

## Методология

Три параллельных саб-агента по независимым направлениям чек-листа skill'а:

1. **Качество unit-тестов и покрытие** — table-driven структура, именование подтестов, полнота ассертов, `go tool cover -func` для поиска непокрытых веток.
2. **Изоляция интеграционных тестов** — build tags, порядок выполнения, `t.Parallel()`, независимость состояния между тестами.
3. **Утечки горутин и гонки** — `go test -race`, паттерны `time.Sleep`-как-синхронизация, отсутствие `t.Cleanup`/`goleak`.

Находки трёх агентов объединены и реализованы в этом проходе.

## Findings

### 1. Отсутствие `goleak.VerifyTestMain` — утечки горутин не проверяются автоматически ✅ исправлено

**Категория:** goroutine-leak-detection · **Severity:** средняя

Пакет активно порождает горутины (`dispatch`, `tenantRefresher`, воркеры пула), но ни один тест не проверял их фактическое завершение — только счастливый путь (`t.Cleanup(m.Stop)` останавливает менеджер, но не убеждается, что все горутины реально вышли до конца процесса тестирования).

**Исправление:** добавлена зависимость `go.uber.org/goleak` (`v1.3.0`), единственный в пакете `TestMain` (`bench_test.go`) обёрнут в `goleak.VerifyTestMain`:

```go
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.DiscardHandler))
	goleak.VerifyTestMain(m)
}
```

Фильтры (`IgnoreCurrent`/`IgnoreAnyFunction`) не потребовались: в проекте нет `otel/sdk`-провайдеров, порождающих фоновые горутины (используются только no-op-реализации). Проверено `go test ./... -race -count=1` и отдельным прогоном `-bench=. -benchtime=1x` — оба чистые, утечек не обнаружено.

### 2. `ExampleNewWorkerManager` мог зависнуть навсегда при регрессии ✅ исправлено

**Категория:** test-hang-risk · **Severity:** низкая-средняя (не влияет на текущее поведение, но превращает будущий баг в CI-зависание вместо быстрого падения)

`example_test.go` ждал результат задачи без таймаута:

```go
fmt.Println(<-done)
// Output: <nil>
```

Если бы регрессия в `dispatch`/`pool` привела к тому, что `Task.Complete` перестал вызываться, этот example завис бы на неопределённое время — `go test` (и CI) заблокировался бы вместо информативного падения по таймауту пакета.

**Исправление:**

```go
select {
case err := <-done:
	fmt.Println(err)
case <-time.After(5 * time.Second):
	fmt.Println("timed out waiting for task completion")
}
// Output: <nil>
```

### 3. `TestStopDoesNotDeadlock` мог оставить горутину-исполнитель висящей при `t.Fatalf` между `Start` и `Stop` ✅ исправлено

**Категория:** goroutine-leak-on-fatal · **Severity:** низкая

Тест вручную создавал и запускал менеджер, но регистрировал `Stop` только как обычный вызов кода теста, а не через `t.Cleanup`:

```go
if err := m.Start(); err != nil {
	t.Fatalf("Start: %v", err)
}

// Запускаем задачи, которые блокируются на ctx.Done().
```

Если бы `t.Fatalf` сработал где-то между этой строкой и ручным `m.Stop()` ниже по телу теста (например, из-за будущей регрессии), тест завершился бы немедленно (`Fatalf` вызывает `runtime.Goexit`), пропустив вызов `Stop()` — воркеры и диспетчер тенанта остались бы работать в фоне до конца всего прогона тестов.

**Исправление:** добавлена страховка сразу после успешного `Start()`:

```go
if err := m.Start(); err != nil {
	t.Fatalf("Start: %v", err)
}

// Stop уже вызывается вручную ниже, но t.Cleanup — страховка на случай
// t.Fatalf между Start и ручным Stop (Stop идемпотентен, см. TestStopIdempotent).
t.Cleanup(m.Stop)
```

`Stop()` идемпотентен (`isStopping.CompareAndSwap`), поэтому двойной вызов (ручной + через `Cleanup`) безопасен — подтверждено существующим `TestStopIdempotent`.

### 4. Семь веток production-кода не были покрыты ни одним тестом ✅ исправлено

**Категория:** coverage-gap · **Severity:** средняя — часть непокрытых веток — это обработка ошибок, специфично требующая внешнего вмешательства (сбой `TenantProvider`), которая при поломке привела бы к тихому проглатыванию ошибки в проде

До этого прохода общее покрытие пакета составляло 89.0%. `go tool cover -func` указывал на конкретные функции с частичным покрытием: `manager.go`: `NewWorkerManager` 77.8%, `Start` 71.4%, `SubmitTask` 95.0%, `tenantRefresher` 87.5%, `refreshTenants` 82.8%, `dispatch` 83.3%; `pool.go`: `addTask` 71.4%.

Корневая причина всех непокрытых веток одна и та же: `mockTenantProvider.err` (`helpers_test.go`) существует специально для инъекции ошибки (`List` возвращает `(m.tenants, m.err)`), но ни один тест до этого прохода не устанавливал его в non-nil значение — сеттера для этого поля вообще не было.

**Исправление:** добавлен сеттер `mockTenantProvider.setErr(err error)` и семь новых регрессионных тестов (`manager_test.go`, `pool_test.go`), каждый нацелен на конкретную непокрытую ветку:

| Ветка | Тест | Что проверяет |
| --- | --- | --- |
| `Start()`, ошибка начального `refreshTenants` (сам пул должен остановиться) | `TestStartInitialRefreshFailure` | `Start` возвращает обёрнутую ошибку провайдера; последующий `pool.addTask` немедленно получает `ErrPoolStopping`, а не зависает |
| `refreshTenants()`, защита `isStopping` после `Stop()` | `TestRefreshTenantsSkipsAfterStop` | вызов `refreshTenants()` после `Stop()` не добавляет новый тенант из обновлённого списка провайдера |
| `refreshTenants()`, пропуск тенанта с `uuid.Nil` | `TestRefreshTenantsSkipsNilID` | тенант с нулевым ID не попадает в `GetTenantIDs()` |
| `refreshTenants()`, дефолт лимита `<= 0` на `1` | `TestRefreshTenantsDefaultsNonPositiveWorkerLimit` | задача тенанта с `WorkerLimit: 0` реально выполняется (а не блокируется навечно на семафоре нулевой ёмкости) |
| `tenantRefresher()`, лог ошибки после успешного старта | `TestTenantRefresherLogsErrorOnListFailure` | после `provider.setErr` на следующем тике `TenantRefreshInterval` в лог попадает `"tenant refresh failed"` |
| `SubmitTask()`, ветка `ErrTenantShuttingDown` | `TestSubmitTaskTenantShuttingDown` | при заполненной очереди тенанта и отменённом `state.ctx` `SubmitTask` детерминированно возвращает `ErrTenantShuttingDown` |
| `dispatch()`, лог при отказе пула без `Complete`-обработчика | `TestDispatchWarnsOnPoolRejectionWithNoCompletionHandler` | заполненная очередь пула + задача без `Complete` → в лог попадает `"pool rejected task with no completion handler"` |
| `pool.addTask()`, `ErrPoolStopping` / `ErrQueueFull` | `TestPoolAddTaskAfterStop`, `TestPoolAddTaskQueueFull` (новый `pool_test.go`) | прямые white-box тесты уровня `pool` (тип неэкспортируемый, поэтому в `package workerpool`) |

Ветки `SubmitTask`'s `ErrTenantShuttingDown` и `dispatch`'s warn-лог детерминированно воспроизведены без искусственных хуков: тенантная очередь намеренно заполняется до отказа (тем же приёмом, что и в уже существующем `TestSubmitTaskQueueFull`), после чего единственный оставшийся готовый `select`-кейс — искомый (`state.ctx.Done()` или отказ `pool.addTask`), что исключает недетерминированность выбора `select` между несколькими готовыми кейсами.

После добавления тестов покрытие выросло с 89.0% до **93.8%**; все перечисленные функции, кроме `NewWorkerManager` (77.8%, непокрытая ветка — ошибка `newPool` при невалидном `RetryPolicy.Attempts`, недостижимая после `Config.Validate()`) и `pool.go`'s `newPool`/`runWorker`/`stop`/`runTask`/`executeWithRetry`/`exponentialBackoff` (частично покрыты через паническую/повторную/таймаут-специфичную логику — вне области этого прохода), достигли 100%.

## Проверено и не является находкой (изменения не требуются)

- **`time.Sleep` как синхронизация** (`manager_test.go:445, 684, 798, 814, 939`) — во всех пяти местах сон используется не для ожидания результата, а для того, чтобы дать фоновой горутине время дойти до внутреннего, не наблюдаемого извне состояния блокировки (`sem.Acquire`) или чтобы подтвердить **отсутствие** второго события (например, что `Complete` не будет вызван повторно). Ни для одного из этих случаев не существует экспонированного сигнала, на который можно было бы опереться в poll-цикле вместо сна — искусственно вводить такой сигнал только ради теста означало бы утечку тестовой детали в production-код. Оставлено как есть; при рефакторинге production-кода в будущем стоит пересмотреть.
- **Хороший poll-цикл** (`manager_test.go:1057-1058`, до правок этого прохода) — образец правильного паттерна ожидания вместо фиксированного сна:

  ```go
  deadline := time.Now().Add(3 * time.Second)
  for time.Now().Before(deadline) && completed.Load() != submitted.Load() {
  	time.Sleep(10 * time.Millisecond)
  }
  ```

  Этот же паттерн переиспользован в новых тестах этого прохода (`TestTenantRefresherLogsErrorOnListFailure`, `TestDispatchWarnsOnPoolRejectionWithNoCompletionHandler`) для ожидания появления строки в захваченном логе.

- **`TestStopIdempotent`** — уже использует хелпер `startManager`, который сам регистрирует `t.Cleanup(m.Stop)`; отдельного фикса не требовалось (в отличие от `TestStopDoesNotDeadlock`, находка №3, который обходит этот хелпер вручную).
- **`errors.Is`-паттерн для sentinel-ошибок** (`executor_registry_test.go`) — все проверки ошибок (`ErrEmptyExecutorKey`, `ErrNilExecutor`, `ErrExecutorAlreadyRegistered`, `ErrExecutorNotFound`) уже используют `errors.Is`, а не сравнение строк или прямое `==`.
- **Именование подтестов** — везде используются именованные `t.Run(name, ...)` с описательными именами; `t.Parallel()` расставлен по всем независимым тестам и подтестам.
- **Изоляция состояния** — каждый тест создаёт собственный `mockTenantProvider`/`WorkerManager`; общих глобальных моков или порядка выполнения, от которого зависели бы тесты, не найдено.
- **Build tags для интеграционных тестов** — не применимо: в пакете нет тестов, требующих внешних зависимостей (БД, сеть) — все тесты по своей природе unit-тесты с in-memory моками, отдельный `integration`-тег не нужен.
- **Race detector** — `go test ./... -race -count=1` проходит чисто как до, так и после всех изменений этого прохода.

## Итог

| Метрика | До | После |
| --- | --- | --- |
| Общее покрытие (`go tool cover -func`) | 89.0% | 93.8% |
| `goleak.VerifyTestMain` | отсутствует | подключён в `bench_test.go` |
| Тесты с риском зависнуть без таймаута | 1 (`ExampleNewWorkerManager`) | 0 |
| Тесты с риском утечки горутины при `t.Fatalf` | 1 (`TestStopDoesNotDeadlock`) | 0 |
| Новые регрессионные тесты | — | 9 (`manager_test.go` ×7, `pool_test.go` ×2) |
