package workerpool

import "github.com/google/uuid"

// HealthStatus — снимок внутреннего состояния WorkerManager в конкретный
// момент времени. Предназначен для liveness/readiness-проб и операционных
// дашбордов.
//
// Tenants[].TenantID раскрывает идентификаторы активных тенантов. Сам пакет
// не выполняет сетевого взаимодействия, поэтому это не уязвимость библиотеки,
// но если HealthStatus транслируется напрямую в неаутентифицированный
// HTTP-эндпоинт (/health, /ready), это позволяет перечислить тенантов извне
// (см. docs/SECURITY_AUDIT.md, находка №3). Аутентифицируйте такой эндпоинт
// или уберите/агрегируйте TenantID перед отдачей наружу.
type HealthStatus struct {
	// Healthy равен true, пока менеджер работает и не находится в процессе
	// остановки. Используйте это поле как основной сигнал живости.
	Healthy bool `json:"healthy"`

	// Stopping равен true после вызова Stop().
	Stopping bool `json:"stopping"`

	// PoolQueueDepth — количество задач, ожидающих в очереди глобального пула.
	// Значение, близкое к PoolQueueCapacity, сигнализирует о backpressure.
	PoolQueueDepth int `json:"pool_queue_depth"`

	// PoolQueueCapacity — максимальная ёмкость очереди глобального пула
	// (Config.TaskQueueSize).
	PoolQueueCapacity int `json:"pool_queue_capacity"`

	// PoolWorkerCount — количество горутин-воркеров в глобальном пуле
	// (Config.WorkerCount).
	PoolWorkerCount int `json:"pool_worker_count"`

	// TenantCount — количество тенантов, отслеживаемых менеджером на момент снимка.
	TenantCount int `json:"tenant_count"`

	// Tenants содержит детальную информацию по каждому тенанту.
	// Порядок элементов не определён.
	Tenants []TenantHealth `json:"tenants"`
}

// TenantHealth — состояние одного тенанта внутри HealthStatus.
type TenantHealth struct {
	// TenantID — идентификатор тенанта.
	TenantID uuid.UUID `json:"tenant_id"`

	// QueueDepth — количество задач, ожидающих в локальном буфере тенанта.
	// Значение, близкое к QueueCapacity, означает, что SubmitTask начнёт
	// возвращать ошибку «очередь заполнена» для этого тенанта.
	QueueDepth int `json:"queue_depth"`

	// QueueCapacity — полная ёмкость буфера задач тенанта
	// (Config.TenantQueueSize).
	QueueCapacity int `json:"queue_capacity"`

	// WorkerLimit — максимальное количество одновременных задач тенанта
	// согласно последнему ответу TenantProvider.
	WorkerLimit int `json:"worker_limit"`
}

// Health возвращает снимок текущего состояния WorkerManager.
//
// Снимок консистентен внутри одного окна RLock, но состояние может измениться
// сразу после возврата. Метод безопасен для конкурентного вызова и не
// блокирует приём задач.
func (w *WorkerManager) Health() HealthStatus {
	stopping := w.isStopping.Load()

	w.tenantsMu.RLock()

	tenants := make([]TenantHealth, 0, len(w.tenants))
	for id, state := range w.tenants {
		tenants = append(tenants, TenantHealth{
			TenantID:      id,
			QueueDepth:    len(state.taskQueue),
			QueueCapacity: cap(state.taskQueue),
			WorkerLimit:   state.limit,
		})
	}

	tenantCount := len(w.tenants)
	w.tenantsMu.RUnlock()

	return HealthStatus{
		Healthy:           !stopping,
		Stopping:          stopping,
		PoolQueueDepth:    len(w.pool.taskChan),
		PoolQueueCapacity: cap(w.pool.taskChan),
		PoolWorkerCount:   w.pool.workerCount,
		TenantCount:       tenantCount,
		Tenants:           tenants,
	}
}
