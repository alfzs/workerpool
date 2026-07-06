package workerpool

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/google/uuid"
)

// TenantProvider поставляет список тенантов, которым должны быть назначены
// воркеры; что делает тенанта пригодным для этого списка — решает
// реализация, workerpool об этом не судит. Реализация должна быть безопасна
// для конкурентного использования и кешировать результаты — менеджер
// вызывает List на каждом тике обновления.
type TenantProvider interface {
	// List возвращает всех тенантов, которым должны быть назначены
	// воркеры. Тенант, отсутствующий в результате, будет удалён из
	// внутреннего состояния менеджера.
	List(ctx context.Context) ([]Tenant, error)
}

// Tenant представляет клиента, чьи задачи выполняются в рамках
// изолированного лимита конкурентности. Реализация должна быть
// безопасна для конкурентного использования.
type Tenant interface {
	// ID возвращает уникальный идентификатор тенанта. Никогда не должен
	// возвращать uuid.Nil.
	ID() uuid.UUID

	// WorkerLimit возвращает максимальное количество одновременно
	// выполняемых задач для этого тенанта. Изменение значения вступает в силу
	// на следующем цикле обновления.
	WorkerLimit() int
}

// TaskExecutor выполняет фактическую работу одного запуска задачи.
// Реализация должна соблюдать отмену контекста и быть безопасна для
// конкурентного вызова из нескольких горутин одновременно.
//
// Значение паники, восстановленной пулом при выполнении Execute, и
// возвращаемая из Execute ошибка логируются пулом целиком, без редактирования
// (см. docs/SECURITY_AUDIT.md, находка №2). Реализации не должны встраивать
// секреты или персональные данные непосредственно в текст panic() или в
// возвращаемые ошибки, если логи пула могут быть доступны сторонам, для
// которых эти данные не предназначены.
type TaskExecutor interface {
	// Execute выполняет работу. Не-nil ошибка инициирует повторную попытку
	// согласно RetryPolicy пула. workerID идентифицирует слот глобального
	// пула, исполняющий задачу (начинается с 0).
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error
}

// tenantState — runtime-состояние одного тенанта, отслеживаемого менеджером.
//
// taskQueue никогда не закрывается: жизненный цикл сигнализируется через
// отмену контекста, что исключает панику при отправке в закрытый канал.
//
// Семафор (sem) ограничивает количество задач тенанта, одновременно
// выполняемых в глобальном пуле. При каждом вызове setWorkerCount создаётся
// новый sem, но перед этим setWorkerCount синхронно дожидается genDone —
// полного завершения диспетчера предыдущего поколения. Инвариант: не более
// одного диспетчера на тенант одновременно читает из taskQueue (см.
// docs/CONCURRENCY_AUDIT.md, находка №1 — до этого инварианта диспетчер
// уходящего поколения мог гонково перехватить задачу нового поколения).
//
// Изменяемые поля (limit, sem, genCancel, genDone) изменяются только под
// tenantsMu менеджера.
type tenantState struct {
	id        uuid.UUID
	taskQueue chan Task

	limit     int
	sem       *semaphore.Weighted
	genCancel context.CancelFunc
	genDone   chan struct{}

	ctx    context.Context //nolint:containedctx // lifecycle context scoping this tenant's dispatcher goroutine, not a per-call context
	cancel context.CancelFunc
}

// WorkerManager управляет выполнением задач для нескольких тенантов.
// Поддерживает по одной горутине-диспетчеру на тенант, которая обеспечивает
// соблюдение лимита конкурентности через взвешенный семафор перед передачей
// задачи в общий глобальный пул.
type WorkerManager struct {
	// logger — переданный через WorkerManagerParams.Logger (или slog.Default(),
	// если не задан), с добавленным атрибутом component=worker_manager.
	logger   *slog.Logger
	provider TenantProvider
	config   Config
	pool     *pool

	ctx    context.Context //nolint:containedctx // lifecycle context scoping the manager's own background goroutines, not a per-call context
	cancel context.CancelFunc

	tenantsMu sync.RWMutex
	tenants   map[uuid.UUID]*tenantState

	executorRegistry *ExecutorRegistry

	wg         sync.WaitGroup
	isStopping atomic.Bool
}

// WorkerManagerParams содержит все зависимости для NewWorkerManager.
type WorkerManagerParams struct {
	// TenantProvider поставляет список тенантов, которым нужны воркеры.
	TenantProvider TenantProvider

	// Config содержит все настраиваемые параметры. Config.Validate()
	// вызывается внутри NewWorkerManager — передавать невалидный конфиг
	// не нужно.
	Config Config

	// ExecutorRegistry, если задан, используется SubmitTask для разрешения
	// Task.ExecutorKey в Task.Executor у задач, которые не заполнили
	// Executor напрямую. Опционально: если задачи всегда приходят с уже
	// заполненным Executor, оставьте nil.
	ExecutorRegistry *ExecutorRegistry

	// Logger используется менеджером и внутренним пулом (с добавленным
	// атрибутом component). Опционально: если не задан, используется
	// slog.Default().
	Logger *slog.Logger
}

// NewWorkerManager создаёт WorkerManager. Возвращает ошибку, если Config
// не прошёл валидацию или не удалось инициализировать глобальный пул.
//
// Логирование: если WorkerManagerParams.Logger не задан, используется
// slog.Default() — единый экземпляр разделяется менеджером и внутренним
// пулом (у каждого свой атрибут component).
// OTel-трассировка и метрики используют глобальные провайдеры: настройте
// otel.SetTracerProvider и otel.SetMeterProvider до старта менеджера.
func NewWorkerManager(p WorkerManagerParams) (*WorkerManager, error) {
	if err := p.Config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	logger := cmp.Or(p.Logger, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())

	pl, err := newPool(poolParams{config: p.Config, logger: logger})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create pool: %w", err)
	}

	return &WorkerManager{
		logger:           logger.With(slog.String("component", "worker_manager")),
		provider:         p.TenantProvider,
		config:           p.Config,
		pool:             pl,
		ctx:              ctx,
		cancel:           cancel,
		tenants:          make(map[uuid.UUID]*tenantState),
		executorRegistry: p.ExecutorRegistry,
	}, nil
}

// Start запускает глобальный пул, загружает начальный список тенантов и
// запускает фоновый цикл обновления.
// Возвращает ошибку, если первичная загрузка тенантов завершилась неудачей —
// в этом случае пул останавливается и все ресурсы освобождаются.
func (w *WorkerManager) Start() error {
	w.pool.start()

	if err := w.refreshTenants(); err != nil {
		w.pool.stop()
		return fmt.Errorf("initial tenant refresh: %w", err)
	}

	w.wg.Add(1)
	go w.tenantRefresher()

	return nil
}

// Stop выполняет штатное завершение WorkerManager.
//
// Последовательность: отмена контекста обновляторя и всех диспетчеров →
// ожидание выхода всех горутин → остановка пула (с соблюдением
// GracefulTimeout и принудительной отменой при его истечении).
//
// Идемпотентен: повторный вызов безопасен.
func (w *WorkerManager) Stop() {
	if w.isStopping.Swap(true) {
		return
	}

	w.logger.Info("stopping worker manager")
	w.cancel()

	// Отменяем контексты тенантов под блокировкой — это создаёт happens-before
	// с refreshTenants, который проверяет isStopping под той же блокировкой перед
	// wg.Add, что исключает гонку wg.Add / wg.Wait.
	w.tenantsMu.Lock()
	for _, t := range w.tenants {
		t.cancel()
	}
	w.tenantsMu.Unlock()

	w.wg.Wait()
	w.pool.stop()

	w.logger.Info("worker manager stopped")
}

// SubmitTask помещает задачу в очередь указанного тенанта.
//
// Возвращает ошибку немедленно, без постановки в очередь, если Task.Ctx
// равен nil, или если ни Task.Executor, ни разрешимый через
// WorkerManagerParams.ExecutorRegistry Task.ExecutorKey не заданы —
// это предотвращает панику глубже в воркере при исполнении задачи.
//
// Вызов неблокирующий: если очередь тенанта заполнена или тенант
// останавливается, немедленно возвращается ошибка без изменения состояния.
// Безопасен для конкурентного вызова из нескольких горутин.
func (w *WorkerManager) SubmitTask(tenantID uuid.UUID, task Task) error {
	if task.Ctx == nil {
		return ErrTaskNilContext
	}

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

	// RLock удерживается вплоть до отправки в taskQueue (не только на время
	// поиска state) — иначе между RUnlock и select возможна гонка с
	// refreshTenants: тенант может быть удалён (state.cancel + drainTaskQueue +
	// delete) в этом промежутке, и задача, отправленная позже в уже
	// осиротевший taskQueue, никогда не будет прочитана (см.
	// docs/CONCURRENCY_AUDIT.md, находка №2). refreshTenants требует
	// эксклюзивный Lock, поэтому удержание RLock здесь исключает гонку;
	// сам select неблокирующий, так что критическая секция не растёт.
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
}

// GetTenantIDs возвращает снимок идентификаторов всех тенантов, которые
// сейчас отслеживаются менеджером. Порядок не определён.
func (w *WorkerManager) GetTenantIDs() []uuid.UUID {
	w.tenantsMu.RLock()
	defer w.tenantsMu.RUnlock()

	ids := make([]uuid.UUID, 0, len(w.tenants))
	for id := range w.tenants {
		ids = append(ids, id)
	}

	return ids
}

// tenantRefresher периодически вызывает refreshTenants и логирует ошибки.
// Завершается при отмене контекста менеджера.
func (w *WorkerManager) tenantRefresher() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.config.TenantRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.refreshTenants(); err != nil {
				w.logger.Error("tenant refresh failed", slog.Any("error", err))
			}
		}
	}
}

// refreshTenants синхронизирует внутреннее состояние тенантов с провайдером.
//
//   - Новые тенанты: создаётся tenantState и запускается диспетчер.
//   - Существующие с изменённым лимитом: диспетчер перезапускается с новым
//     семафором; старый диспетчер завершается штатно по мере закрытия задач.
//   - Удалённые тенанты: контекст тенанта отменяется, диспетчер останавливается.
//     Задачи в буфере taskQueue удаляются — они нерелевантны для несуществующего
//     тенанта.
//
// Удерживает tenantsMu на протяжении всего обновления.
func (w *WorkerManager) refreshTenants() error {
	tenants, err := w.provider.List(w.ctx)
	if err != nil {
		return fmt.Errorf("list tenants: %w", err)
	}

	w.tenantsMu.Lock()
	defer w.tenantsMu.Unlock()

	// Защита от вызова после Stop().
	if w.isStopping.Load() {
		return nil
	}

	wantSet := make(map[uuid.UUID]struct{}, len(tenants))

	for _, t := range tenants {
		id := t.ID()
		if id == uuid.Nil {
			w.logger.Warn("skipping tenant with nil id")
			continue
		}

		wantSet[id] = struct{}{}

		limit := t.WorkerLimit()
		if limit <= 0 {
			w.logger.Warn("invalid worker limit, using 1",
				slog.String("tenant_id", id.String()),
				slog.Int("limit", limit))
			limit = 1
		}

		if state, exists := w.tenants[id]; exists {
			w.setWorkerCount(state, limit)
		} else {
			w.createTenant(id, limit)
		}
	}

	// Удаляем тенантов, отсутствующих в списке, полученном от TenantProvider.
	for id, state := range w.tenants {
		if _, exists := wantSet[id]; !exists {
			state.cancel()

			dropped := w.drainTaskQueue(state)
			delete(w.tenants, id)

			if dropped > 0 {
				w.logger.Debug("tenant removed, tasks dropped",
					slog.String("tenant_id", id.String()),
					slog.Int("dropped", dropped))
			}
		}
	}

	return nil
}

// drainTaskQueue вычитывает оставшиеся задачи из state.taskQueue и вызывает
// для каждой Task.Complete(ErrDispatcherStopped). Вызывается после
// state.cancel() при удалении тенанта: диспетчер (dispatch) в этот момент
// либо уже завершается, либо конкурентно читает из того же канала — раз
// каждая задача достаётся ровно одному получателю, состязание за конкретную
// задачу не приводит к двойному вызову Complete. Возвращает число слитых задач.
func (w *WorkerManager) drainTaskQueue(state *tenantState) int {
	dropped := 0

	for {
		select {
		case task := <-state.taskQueue:
			dropped++

			if task.Complete != nil {
				task.Complete(ErrDispatcherStopped)
			}
		default:
			return dropped
		}
	}
}

// createTenant инициализирует новый tenantState и запускает диспетчер.
// Вызывается под tenantsMu.
func (w *WorkerManager) createTenant(id uuid.UUID, limit int) {
	ctx, cancel := context.WithCancel(w.ctx)
	state := &tenantState{
		id:        id,
		taskQueue: make(chan Task, w.config.TenantQueueSize),
		ctx:       ctx,
		cancel:    cancel,
	}
	w.tenants[id] = state
	w.setWorkerCount(state, limit)
}

// setWorkerCount перезапускает диспетчер тенанта с семафором нового размера.
// Предыдущий диспетчер (если есть) отменяется через genCancel, после чего
// setWorkerCount синхронно дожидается genDone — полного выхода диспетчера
// предыдущего поколения — и только затем поднимает новый. Это гарантирует,
// что из state.taskQueue в любой момент читает не более одного диспетчера:
// без этого ожидания диспетчер уходящего поколения мог по гонке (select
// недетерминирован между готовыми кейсами) перехватить задачу, адресованную
// новому поколению, и немедленно завершить её с ErrDispatcherStopped вместо
// выполнения под новым лимитом (см. docs/CONCURRENCY_AUDIT.md, находка №1).
// Ожидание ограничено по времени: dispatch не содержит блокирующих вызовов,
// не учитывающих genCtx, поэтому предыдущий диспетчер завершается практически
// сразу после genCancel(). Задачи в глобальном пуле при этом не прерываются.
//
// Вызывается под tenantsMu.
func (w *WorkerManager) setWorkerCount(state *tenantState, limit int) {
	if state.limit == limit && state.genCancel != nil {
		return
	}

	if state.genCancel != nil {
		state.genCancel()
		<-state.genDone
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
}

// dispatch — единственная горутина-диспетчер на тенанта, обеспечивающая
// соблюдение лимита конкурентности. Заменяет прежнюю схему N блокирующих
// воркеров на тенанта: количество горутин сокращается с O(сумма лимитов)
// до O(количество тенантов).
//
// Для каждой задачи:
//  1. Ожидает задачу из taskQueue (или завершается по genCtx).
//  2. Захватывает слот семафора — блокируется, если лимит исчерпан.
//  3. Оборачивает Complete для освобождения слота после завершения задачи.
//  4. Передаёт задачу в глобальный пул.
func (w *WorkerManager) dispatch(genCtx context.Context, state *tenantState, sem *semaphore.Weighted) {
	defer w.wg.Done()

	for {
		// Шаг 1: ожидание задачи.
		var task Task
		select {
		case <-genCtx.Done():
			return
		case task = <-state.taskQueue:
		}

		// Шаг 2: захват слота конкурентности.
		if err := sem.Acquire(genCtx, 1); err != nil {
			// genCtx отменён во время ожидания слота.
			if task.Complete != nil {
				task.Complete(ErrDispatcherStopped)
			}

			return
		}

		// Шаг 3: освобождение слота через Complete.
		original := task.Complete
		task.Complete = func(err error) {
			sem.Release(1)

			if original != nil {
				original(err)
			}
		}

		// Шаг 4: передача в глобальный пул. Правило единственной обработки:
		// если у задачи есть исходный получатель (original), ошибка передаётся
		// ему через Complete; лог здесь — только запасной канал на случай,
		// когда обработать ошибку больше некому.
		if err := w.pool.addTask(task); err != nil {
			if original == nil {
				w.logger.Warn("pool rejected task with no completion handler",
					slog.String("tenant_id", state.id.String()),
					slog.Any("error", err))
			}

			task.Complete(err)
		}
	}
}
