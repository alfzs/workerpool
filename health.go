package workerpool

import "github.com/google/uuid"

// HealthStatus — снимок внутреннего состояния WorkerManager в конкретный
// момент времени. Предназначен для liveness/readiness-проб и операционных
// дашбордов.
type HealthStatus struct {
	// Healthy равен true, пока менеджер работает и не находится в процессе
	// остановки. Используйте это поле как основной сигнал живости.
	Healthy bool

	// Stopping равен true после вызова Stop().
	Stopping bool

	// PoolQueueDepth — количество задач, ожидающих в очереди глобального пула.
	// Значение, близкое к PoolQueueCapacity, сигнализирует о backpressure.
	PoolQueueDepth int

	// PoolQueueCapacity — максимальная ёмкость очереди глобального пула
	// (Config.TaskQueueSize).
	PoolQueueCapacity int

	// PoolWorkerCount — количество горутин-воркеров в глобальном пуле
	// (Config.WorkerCount).
	PoolWorkerCount int

	// TenantCount — количество активных тенантов на момент снимка.
	TenantCount int

	// Tenants содержит детальную информацию по каждому тенанту.
	// Порядок элементов не определён.
	Tenants []TenantHealth
}

// TenantHealth — состояние одного тенанта внутри HealthStatus.
type TenantHealth struct {
	// TenantID — идентификатор тенанта.
	TenantID uuid.UUID

	// QueueDepth — количество задач, ожидающих в локальном буфере тенанта.
	// Значение, близкое к QueueCapacity, означает, что SubmitTask начнёт
	// возвращать ошибку «очередь заполнена» для этого тенанта.
	QueueDepth int

	// QueueCapacity — полная ёмкость буфера задач тенанта
	// (Config.TenantQueueSize).
	QueueCapacity int

	// WorkerLimit — максимальное количество одновременных задач тенанта
	// согласно последнему ответу TenantProvider.
	WorkerLimit int
}

// Health возвращает снимок текущего состояния WorkerManager.
//
// Снимок консистентен внутри одного окна RLock, но состояние может измениться
// сразу после возврата. Метод безопасен для конкурентного вызова и не
// блокирует приём задач.
func (w *WorkerManager) Health() HealthStatus {
	stopping := w.stopping.Load()

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
