# Аудит использования context.Context

Аудит по чек-листу `/cc-skills-golang:golang-context`: распространение контекста через
весь жизненный цикл запроса, позиция и имя параметра `ctx`, отсутствие контекста
в структурах (кроме обоснованных исключений), запрет на `nil`-контекст,
обязательный вызов `cancel()` на всех путях, `context.Background()` только на
верхнем уровне, ключи значений контекста, `context.WithoutCancel`.

## Находки

### 1. `pool.recordCompletion` использовал `context.Background()` вместо доступного `ctx` со span'ом ✅ исправлено

**Категория:** context-propagation · **Severity:** низкая — не влияет на корректность
выполнения задач, только на observability (потеря связи метрики с трейсом через
exemplar'ы у экспортёров, которые их поддерживают, например Prometheus+OTel с
exemplar storage).

`executeWithRetry` (`pool.go`) создаёт `taskCtx` (дочерний от `task.Ctx`) и оборачивает
его в OTel-span (`ctx, span := p.tracer.Start(taskCtx, ...)`), затем передаёт `ctx`
в `task.Executor.Execute(ctx, ...)` — корректное распространение. Но на всех трёх
путях завершения (`success`, `cancelled`, `failed`) `recordCompletion` вызывался без
контекста и использовал `context.Background()` внутри себя:

```go
func (p *pool) recordCompletion(task Task, dur time.Duration, status string) {
	attrs := metric.WithAttributes(
		attribute.String("tenant.id", task.TenantID.String()),
		attribute.String("status", status),
	)
	p.taskDuration.Record(context.Background(), dur.Seconds(), attrs)
	p.tasksTotal.Add(context.Background(), 1, attrs)
}
```

Это нарушение принципа «тот же контекст должен распространяться через весь
жизненный цикл запроса»: на пути `success`, например, вызов происходит сразу после
`task.Executor.Execute(ctx, ...)`, пока span ещё активен (`span.End()` — только в
`defer`, после возврата из `executeWithRetry`) — валидный `ctx` со span'ом доступен
в области видимости, но игнорировался без причины.

**Исправление:** `recordCompletion` теперь принимает `ctx context.Context` первым
параметром; все три вызова передают span-контекст (`ctx`), а не пересоздают
`context.Background()`:

```go
func (p *pool) recordCompletion(ctx context.Context, task Task, dur time.Duration, status string) {
	attrs := metric.WithAttributes(
		attribute.String("tenant.id", task.TenantID.String()),
		attribute.String("status", status),
	)
	p.taskDuration.Record(ctx, dur.Seconds(), attrs)
	p.tasksTotal.Add(ctx, 1, attrs)
}
```

На пути `cancelled` переданный `ctx` (производный от уже отменённого `taskCtx`) сам
уже отменён — это безопасно: `metric.Meter.Record`/`Add` не блокируются на контексте
и не возвращают ошибку при его отмене, только теряют exemplar (что семантически
корректно — отменённой операции не должно быть привязано валидное измерение).

## Проверено и не вызывает замечаний

- **`ctx` — первый параметр, названный `ctx`** — во всех публичных сигнатурах
  (`TenantProvider.List(ctx context.Context)`, `TaskExecutor.Execute(ctx context.Context, ...)`)
  соблюдается без исключений.
- **`nil`-контекст** — `SubmitTask` явно проверяет `task.Ctx == nil` и возвращает
  `ErrTaskNilContext` до того, как задача попадёт куда-либо глубже — корректная
  защита на границе пакета.
- **Контекст в структурах** — три случая хранения `context.Context` как поля
  (`tenantState.ctx`, `WorkerManager.ctx`, `pool.forceCtx`), все три уже помечены
  `//nolint:containedctx` с пояснением: это контексты жизненного цикла компонентов
  (тенанта, менеджера, пула), а не контексты конкретного вызова — то есть намеренное,
  документированное исключение, а не пропущенное нарушение. `Task.Ctx` — четвёртый
  случай, обоснован отдельно (Task — конверт данных, передаваемый по значению через
  каналы, а не долгоживущий объект).
- **`context.Background()` вне request-пути** — три места (`manager.go:139` —
  корневой контекст `WorkerManager`, `pool.go:98` — корневой контекст `forceCtx`
  пула, `doc.go:58` — пример в godoc) создают `context.Background()` только как
  корень жизненного цикла долгоживущего компонента или в примере использования —
  не в середине пути обработки запроса/задачи. Аналогично `net/http.Server` или
  `database/sql.DB`, которые тоже не принимают внешний контекст на весь жизненный
  цикл, а управляются явными методами (`Shutdown`/`Close`) — здесь эту роль играет
  `WorkerManager.Stop()`.
- **`cancel()` вызывается на всех путях** — пример в `doc.go` вызывает `cancel()`
  как в колбэке `Complete` (успех/ошибка выполнения), так и на пути, когда
  `SubmitTask` вернула ошибку до постановки в очередь. `executeWithRetry` вызывает
  `taskCancel()` через `defer` сразу после создания — покрывает все ветки выхода
  (включая панику, перехватываемую выше по стеку в `runTask`).
- **`context.WithValue` / значения контекста** — нигде в пакете не используется;
  не является находкой, так как у пакета нет request-scoped метаданных (ID
  пользователя, correlation ID и т.п.), которые было бы уместно передавать через
  контекст, а не явными параметрами (`TenantID`, `TaskID` уже передаются как поля
  `Task`, что и есть предпочтительная альтернатива согласно чек-листу).
- **`context.WithoutCancel`** — не используется; не является находкой. Единственная
  фоновая работа, переживающая породивший её контекст, — это OTel-экспорт метрик
  через глобальный `MeterProvider`, который уже устроен так вызывающим кодом
  (приложением, настраивающим провайдер), а не этим пакетом.
- **Распространение в БД/внешние вызовы** — пакет не выполняет прямых операций с БД
  или HTTP; единственная внешняя точка — `TenantProvider.List(ctx)`, вызываемая с
  `w.ctx` (жизненный цикл менеджера) — корректно, так как отмена менеджера должна
  прерывать и висящий вызов `List`.
