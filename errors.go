package workerpool

import "errors"

// Ошибки глобального пула. Проверяются через errors.Is.
var (
	// ErrPoolStopping возвращается addTask, если пул уже в процессе остановки.
	// Постоянная ошибка — повторная попытка не поможет.
	ErrPoolStopping = errors.New("workerpool: pool is stopping")

	// ErrQueueFull возвращается addTask, если очередь глобального пула
	// заполнена. Временная ошибка — вызывающий код может повторить попытку.
	ErrQueueFull = errors.New("workerpool: pool queue full")
)

// Ошибки WorkerManager. Проверяются через errors.Is.
var (
	// ErrTenantNotFound возвращается SubmitTask, если тенант с указанным ID
	// отсутствует в последнем снимке TenantProvider.List. Постоянная
	// ошибка, пока тенант не появится в очередном обновлении.
	ErrTenantNotFound = errors.New("workerpool: tenant not found")

	// ErrTenantShuttingDown возвращается SubmitTask, если тенант уже удалён
	// из списка TenantProvider и его диспетчер завершается. Постоянная ошибка.
	ErrTenantShuttingDown = errors.New("workerpool: tenant is shutting down")

	// ErrTenantQueueFull возвращается SubmitTask, если буфер задач тенанта
	// заполнен. Временная ошибка — вызывающий код может повторить попытку.
	ErrTenantQueueFull = errors.New("workerpool: tenant queue full")

	// ErrDispatcherStopped передаётся через Task.Complete, если диспетчер
	// тенанта остановился до захвата слота конкурентности для задачи.
	ErrDispatcherStopped = errors.New("workerpool: dispatcher stopped before task could run")
)

// Ошибки ExecutorRegistry. Проверяются через errors.Is.
var (
	// ErrEmptyExecutorKey возвращается Register при попытке зарегистрировать
	// executor с пустым ключом.
	ErrEmptyExecutorKey = errors.New("workerpool: executor key cannot be empty")

	// ErrNilExecutor возвращается Register при попытке зарегистрировать nil
	// в качестве executor'а.
	ErrNilExecutor = errors.New("workerpool: executor cannot be nil")

	// ErrExecutorAlreadyRegistered возвращается Register, если ключ уже занят.
	ErrExecutorAlreadyRegistered = errors.New("workerpool: executor already registered")

	// ErrExecutorNotFound возвращается Get, если ключ не зарегистрирован.
	ErrExecutorNotFound = errors.New("workerpool: executor not found")
)
