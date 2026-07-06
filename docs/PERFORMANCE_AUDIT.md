# Аудит производительности

Аудит по чек-листу `/cc-skills-golang:golang-performance`: профилирование
через `pprof` (`-memprofile`/`-cpuprofile` на `BenchmarkSubmitTask`, см.
`docs/BENCHMARK_AUDIT.md` за происхождением бенчмарка), а не гадание —
находки ниже основаны на реальных данных `alloc_objects`, а не на интуиции.

## Находки

Находок, требующих исправления кода в самом пакете, в этом раунде нет —
единственный кандидат на оптимизацию (ниже) рассмотрен, измерен и
осознанно отклонён. Единственное применённое изменение — тестовая
инфраструктура (см. «Побочное изменение» ниже), необходимая для самого
измерения, а не оптимизация продакшен-кода.

## Рассмотрено и отклонено

### `task.TenantID.String()` вычисляется дважды на каждую задачу — оптимизация отклонена

**Категория:** repeated-expensive-work · **Severity:** низкая — не баг,
дублирование работы на горячем пути, но исправление того не стоит.

`go tool pprof -top -alloc_objects mem.prof` после
`go test -bench=BenchmarkSubmitTask -memprofile=mem.prof` показал
`uuid.UUID.String` как **аллокатор №1** — 15.12% всех аллокаций
(`1365395` из `9029018` объектов), опережающий даже `context.WithDeadlineCause`
и создание OTel-спанов. Причина: `executeWithRetry` вызывает
`task.TenantID.String()` в атрибутах span'а, а затем `recordCompletion`
(вызываемый на каждом исходе — success/cancelled/failed) вызывает
`task.TenantID.String()` заново из той же самой `task.TenantID` — одна и
та же 36-символьная строка аллоцируется дважды за одно выполнение задачи.

```go
// pool.go, executeWithRetry — ДО
ctx, span := p.tracer.Start(taskCtx, "workerpool.task.execute",
    trace.WithAttributes(
        attribute.String("tenant.id", task.TenantID.String()),
        ...
    ))
...
p.recordCompletion(ctx, task, time.Since(start), "success")

// pool.go, recordCompletion — ДО
func (p *pool) recordCompletion(ctx context.Context, task Task, dur time.Duration, status string) {
    attrs := metric.WithAttributes(
        attribute.String("tenant.id", task.TenantID.String()), // повторный вызов
        ...
    )
    ...
}
```

```go
// pool.go, executeWithRetry — ПОСЛЕ
tenantIDStr := task.TenantID.String()

ctx, span := p.tracer.Start(taskCtx, "workerpool.task.execute",
    trace.WithAttributes(
        attribute.String("tenant.id", tenantIDStr),
        ...
    ))
...
p.recordCompletion(ctx, tenantIDStr, time.Since(start), "success")

// pool.go, recordCompletion — ПОСЛЕ
func (p *pool) recordCompletion(ctx context.Context, tenantIDStr string, dur time.Duration, status string) {
    attrs := metric.WithAttributes(
        attribute.String("tenant.id", tenantIDStr),
        ...
    )
    ...
}
```

**Рассмотренное исправление** (реализовано, измерено, затем отменено):
`tenantIDStr := task.TenantID.String()` вычисляется один раз в начале
`executeWithRetry` и передаётся во все три сайта вызова
`recordCompletion` (success/cancelled/failed) вместо всей `Task`;
сигнатура `recordCompletion` меняется на приём готовой строки вместо
`Task`.

**Измерено `benchstat` (`-count=10` до/после, единственная затронутая
цель — `BenchmarkSubmitTask`):**

```
goos: linux
goarch: amd64
cpu: Intel(R) Core(TM) i3-6300 CPU @ 3.80GHz

              │  report-1.txt (baseline)  │        report-2.txt (after)        │
              │          sec/op           │   sec/op     vs base                │
SubmitTask-4              2.755µ ± 12%       2.706µ ± 10%  ~ (p=0.353 n=10)

              │  report-1.txt (baseline)  │        report-2.txt (after)        │
              │           B/op            │   B/op     vs base                  │
SubmitTask-4              1.398Ki ± 0%       1.352Ki ± 0%  -3.35% (p=0.000 n=10)

              │  report-1.txt (baseline)  │        report-2.txt (after)        │
              │        allocs/op          │  allocs/op  vs base                 │
SubmitTask-4               23.00 ± 0%         22.00 ± 0%  -4.35% (p=0.000 n=10)
```

`sec/op` **не** является статистически значимым улучшением (`p=0.353`,
помечено `~`) — устранение одной аллокации из 23 тонет в шуме
планировщика/OTel на этом железе. `B/op` (-3.35%) и `allocs/op` (-4.35%)
— статистически значимы (`p=0.000`), что соответствует ожиданию:
устранена ровно одна аллокация 36-байтовой строки на задачу.

**Решение: не применять.** Изменение сигнатуры `recordCompletion` с
`(ctx, task Task, dur, status)` на `(ctx, tenantIDStr string, dur,
status)` обменивает единственную некритичную аллокацию (значимо только
в `B/op`/`allocs/op`, не в реальной задержке) на потерю читаемости на
каждом из трёх сайтов вызова: `p.recordCompletion(ctx, tenantIDStr, ...)`
не даёт очевидного ответа, что такое `tenantIDStr` и откуда он взят,
тогда как `p.recordCompletion(ctx, task, ...)` явно говорит — «данные
берутся из задачи». Читателю пришлось бы подниматься на несколько строк
вверх к объявлению `tenantIDStr := task.TenantID.String()`, чтобы
восстановить эту связь. Цена (неявность происхождения значения на
горячем, часто читаемом пути) не окупается выигрышем (аллокация без
подтверждённого влияния на латентность) — правка отменена, `pool.go`
оставлен как есть.

### Побочное изменение: тестовый логгер отключён на время бенчмарков ✅ применено

До этого раунда `BenchmarkSubmitTask` (через `startManager`/`t.Cleanup(m.Stop)`)
писал INFO-логи через `slog.Default()` при каждой остановке менеджера
между повторами `-count=N`; при перенаправлении вывода команды в файл
(`go test -bench=... | tee report.txt`) эти строки перемежались с
результирующими строками бенчмарка и ломали парсер `benchstat`
(`parsing iteration count: invalid syntax`). Добавлен `TestMain` в
`bench_test.go`, отключающий вывод логгера по умолчанию
(`slog.SetDefault(slog.New(slog.DiscardHandler))`) на время прогона
тестового бинарника пакета — сама работа логирования по-прежнему
происходит (звонок в `Stop()` синхронный и вне таймируемого участка
`b.RunParallel`), меняется только пункт назначения вывода. Ни один
существующий тест не проверяет вывод логгера по умолчанию (проверено
`grep` по `*_test.go`), так что регрессии в остальном тестовом наборе
нет — подтверждено полным прогоном `-race`.

## Проверено и не вызывает замечаний

### fieldalignment: 8 находок рассмотрены, применена только оценка — ни одна не исправлена

`fieldalignment ./...` сообщил о возможном сокращении «pointer bytes»
(GC-сканируемого префикса структуры) в 8 местах: `Task` (`pool.go:25`,
88→48), `pool` (`pool.go:57`, 240→88), `poolParams` (`pool.go:89`, 96→8),
`tenantState` (`manager.go:75`, 80→56), `WorkerManager` (`manager.go:92`,
184→72), `WorkerManagerParams` (`manager.go:113`, 120→32),
`executor_registry.go:16` (32→8), `health.go:15` (48→8), плюс структуры
в тестовых файлах.

Проверка по принципу «профилировать, прежде чем оптимизировать»:
структуры, конструируемые один раз при старте или один раз на тенанта
(`pool`, `poolParams`, `tenantState`, `WorkerManager`,
`WorkerManagerParams`, структуры `executor_registry.go`/`health.go`) и
доступные затем только через указатель — не в горячем пути, переупорядочивание
их полей не даст измеримого эффекта на рантайм. Единственный кандидат —
`Task` (`pool.go:25`): конструируется на каждую задачу и копируется по
значению через каналы/фреймы вызова (`dispatch`→`addTask`→`runTask`→
`executeWithRetry`).

Тем не менее переупорядочивание `Task` **не применено**: отдельная
проверка через `unsafe.Sizeof` (переставлены поля так, чтобы указатели/
интерфейсы/функции — `Ctx`, `Executor`, `Complete`, `ExecutorKey` —
шли перед `TaskID`/`TenantID`, оба `uuid.UUID` = `[16]byte` без
указателей) показала, что **общий размер структуры не меняется** (88
байт в обоих вариантах) — рекомендация `fieldalignment` касается только
сокращения GC-сканируемого префикса, а не размера копии, передаваемой по
каналу. Без отдельного бенчмарка, изолирующего стоимость копирования/
отправки в канал именно этого префикса, нет свидетельств, что
переупорядочивание изменит что-то измеримое — применять правку
исключительно по показанию линтера, без данных, противоречило бы
принципу этого раунда. Зафиксировано как проверено-но-не-применено;
может быть переоценено, если появится целевой бенчмарк.

### OTel/context-накладные расходы — неотъемлемая стоимость, не находка

Профиль (`-memprofile`) также показал заметный вклад
`context.WithDeadlineCause`/`context.WithValue`/`context.AfterFunc`
(дедлайн и принудительная отмена задачи — `context.WithTimeout(task.Ctx,
p.config.TaskTimeout)` в `executeWithRetry`), `time.newTimer` (таймер
повторной попытки) и `otel/metric.WithAttributes`/
`otel/attribute.computeDataFixed`/`tracer.newSpan`/`otel/trace.WithAttributes`
(создание span'а и метрик на каждую задачу). Ни один из этих аллокаторов
не устраним без изменения поведения: дедлайн и принудительная отмена —
корректностные гарантии пакета (см. `doc.go`, «Корректное завершение»),
трассировка на каждый вызов — осознанное архитектурное решение,
подтверждённое отдельно в `docs/OBSERVABILITY_AUDIT.md`. `uuid.NewRandomFromReader`
в профиле принадлежит вспомогательной функции бенчмарка `newTask()`
(генерация нового `TaskID` на каждую итерацию) — это реалистичное
поведение продакшена, а не артефакт теста, поэтому тоже не находка.

### Не применимо

- **GOMEMLIMIT/GOGC-тюнинг, PGO** — уже отмечено как неприменимое в
  `docs/MODERNIZE_AUDIT.md`: у пакета нет `main`-пакета/точки входа,
  настройка рантайма и профиль для PGO — забота процесса-хоста,
  встраивающего библиотеку.
- **HTTP-транспорт (`MaxIdleConnsPerHost` и т.п.)** — пакет не создаёт
  HTTP-клиентов; сетевые вызовы (если есть) находятся внутри
  реализации `TaskExecutor`, предоставляемой вызывающим кодом.
- **`unsafe`** — не оправдан: нет профилированного горячего пути, где
  выигрыш подтверждён бенчмарком на >10% (требование чек-листа для
  использования `unsafe`).

## Проверка

`gofmt -l .` — пусто; `go build ./...`, `go vet ./...` — чисто;
`golangci-lint run ./...` — 0 issues; `go test ./... -race -count=1
-timeout 90s` — ok.
