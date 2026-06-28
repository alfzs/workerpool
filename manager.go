package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/google/uuid"
)

// TenantProvider поставляет список активных тенантов.
// Реализация должна быть безопасна для конкурентного использования и
// кешировать результаты — менеджер вызывает GetActive на каждом тике
// обновления.
type TenantProvider interface {
	// GetActive возвращает всех тенантов, которым должны быть назначены
	// воркеры. Тенант, отсутствующий в результате, будет удалён из
	// внутреннего состояния менеджера.
	GetActive(ctx context.Context) ([]Tenant, error)
}

// Tenant представляет клиента, чьи задачи выполняются в рамках
// изолированного лимита конкурентности. Реализация должна быть
// безопасна для конкурентного использования.
type Tenant interface {
	// GetID возвращает уникальный идентификатор тенанта. Никогда не должен
	// возвращать uuid.Nil.
	GetID() uuid.UUID

	// GetWorkerLimit возвращает максимальное количество одновременно
	// выполняемых задач для этого тенанта. Изменение значения вступает в силу
	// на следующем цикле обновления.
	GetWorkerLimit() int
}

// TaskExecutor выполняет фактическую работу одного запуска задачи.
// Реализация должна соблюдать отмену контекста и быть безопасна для
// конкурентного вызова из нескольких горутин одновременно.
type TaskExecutor interface {
	// Execute выполняет работу. Не-nil ошибка инициирует повторную попытку
	// согласно RetryPolicy пула. workerID идентифицирует слот глобального
	// пула, исполняющий задачу (начинается с 0).
	Execute(ctx context.Context, tenantID uuid.UUID, workerID int) error
}

// tenantState — runtime-состояние одного активного тенанта.
//
// taskQueue никогда не закрывается: жизненный цикл сигнализируется через
// отмену контекста, что исключает панику при отправке в закрытый канал.
//
// Семафор (sem) ограничивает количество задач тенанта, одновременно
// выполняемых в глобальном пуле. При каждом вызове setWorkerCount создаётся
// новый sem; старый диспетчер освобождает слоты старого sem через Complete-
// коллбэки по мере завершения задач — конфликтов между поколениями нет.
//
// Изменяемые поля (limit, sem, genCancel) изменяются только под
// tenantsMu менеджера.
type tenantState struct {
	id        uuid.UUID
	taskQueue chan Task

	limit     int
	sem       *semaphore.Weighted
	genCancel context.CancelFunc

	ctx    context.Context
	cancel context.CancelFunc
}

// WorkerManager управляет выполнением задач для нескольких тенантов.
// Поддерживает по одной горутине-диспетчеру на тенант, которая обеспечивает
// соблюдение лимита конкурентности через взвешенный семафор перед передачей
// задачи в общий глобальный пул.
type WorkerManager struct {
	// logger инициализируется из slog.Default() с атрибутом component.
	logger   *slog.Logger
	provider TenantProvider
	config   Config
	pool     *pool

	ctx    context.Context
	cancel context.CancelFunc

	tenantsMu sync.RWMutex
	tenants   map[uuid.UUID]*tenantState

	wg       sync.WaitGroup
	stopping atomic.Bool
}

// WorkerManagerParams содержит все зависимости для NewWorkerManager.
type WorkerManagerParams struct {
	// TenantProvider поставляет список активных тенантов.
	TenantProvider TenantProvider

	// Config содержит все настраиваемые параметры. Config.Validate()
	// вызывается внутри NewWorkerManager — передавать невалидный конфиг
	// не нужно.
	Config Config
}

// NewWorkerManager создаёт WorkerManager. Возвращает ошибку, если Config
// не прошёл валидацию или не удалось инициализировать глобальный пул.
//
// Логирование ведётся через slog.Default() — настройте глобальный логгер
// до вызова этой функции.
// OTel-трассировка и метрики используют глобальные провайдеры: настройте
// otel.SetTracerProvider и otel.SetMeterProvider до старта менеджера.
func NewWorkerManager(p WorkerManagerParams) (*WorkerManager, error) {
	if err := p.Config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	pl, err := newPool(poolParams{config: p.Config})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create pool: %w", err)
	}

	return &WorkerManager{
		logger:   slog.Default().With(slog.String("component", "worker_manager")),
		provider: p.TenantProvider,
		config:   p.Config,
		pool:     pl,
		ctx:      ctx,
		cancel:   cancel,
		tenants:  make(map[uuid.UUID]*tenantState),
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
	if w.stopping.Swap(true) {
		return
	}

	w.logger.Info("stopping worker manager")
	w.cancel()

	// Отменяем контексты тенантов под блокировкой — это создаёт happens-before
	// с refreshTenants, который проверяет stopping под той же блокировкой перед
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
// Вызов неблокирующий: если очередь тенанта заполнена или тенант
// останавливается, немедленно возвращается ошибка без изменения состояния.
// Безопасен для конкурентного вызова из нескольких горутин.
func (w *WorkerManager) SubmitTask(tenantID uuid.UUID, task Task) error {
	w.tenantsMu.RLock()
	state, ok := w.tenants[tenantID]
	w.tenantsMu.RUnlock()

	if !ok {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	select {
	case state.taskQueue <- task:
		return nil
	case <-state.ctx.Done():
		return fmt.Errorf("tenant %s is shutting down", tenantID)
	default:
		return fmt.Errorf("tenant %s queue full (capacity %d)", tenantID, cap(state.taskQueue))
	}
}

// GetActiveTenants возвращает снимок идентификаторов всех активных тенантов.
// Порядок не определён.
func (w *WorkerManager) GetActiveTenants() []uuid.UUID {
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
	active, err := w.provider.GetActive(w.ctx)
	if err != nil {
		return err
	}

	w.tenantsMu.Lock()
	defer w.tenantsMu.Unlock()

	// Защита от вызова после Stop().
	if w.stopping.Load() {
		return nil
	}

	activeSet := make(map[uuid.UUID]struct{}, len(active))

	for _, t := range active {
		id := t.GetID()
		if id == uuid.Nil {
			w.logger.Warn("skipping tenant with nil id")
			continue
		}
		activeSet[id] = struct{}{}

		limit := t.GetWorkerLimit()
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

	// Удаляем тенантов, отсутствующих в активном множестве.
	for id, state := range w.tenants {
		if _, exists := activeSet[id]; !exists {
			dropped := len(state.taskQueue)
			state.cancel()
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
// Предыдущий диспетчер (если есть) отменяется через genCancel: он завершается
// после того, как текущий Acquire возвращает ошибку, а слоты семафора
// старого поколения освобождаются через Complete-коллбэки активных задач.
// Задачи в глобальном пуле при этом не прерываются.
//
// Вызывается под tenantsMu.
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
	go w.dispatch(state, sem, genCtx)
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
func (w *WorkerManager) dispatch(state *tenantState, sem *semaphore.Weighted, genCtx context.Context) {
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
				task.Complete(fmt.Errorf("dispatcher stopped before task could run"))
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

		// Шаг 4: передача в глобальный пул.
		if err := w.pool.addTask(task); err != nil {
			w.logger.Warn("pool rejected task",
				slog.String("tenant_id", state.id.String()),
				slog.Any("error", err))
			task.Complete(err)
		}
	}
}
