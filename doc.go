/*
Пакет workerpool реализует пул воркеров с изоляцией по тенантам, ограничением
конкурентности на тенанта, трассировкой через OpenTelemetry и корректным
graceful shutdown.

# Компоненты

Пакет состоит из трёх взаимодействующих частей:

  - WorkerManager — управление жизненным циклом тенантов и контроль
    конкурентности через взвешенный семафор.
  - Pool — общий пул горутин фиксированного размера: исполняет задачи,
    снимает OTel-метрики и трассировку, реализует повторные попытки.
  - ExecutorRegistry — сопоставляет строковые ключи (хранящиеся в job store)
    с конкретными реализациями TaskExecutor.

Для персистентного планирования и multi-instance развёртывания пакет
спроектирован под использование совместно с River (github.com/riverqueue/river)
в роли job store. Полная схема интеграции описана в ARCHITECTURE.md.

# Быстрый старт

	cfg := workerpool.Config{
		WorkerCount:           64,
		TaskQueueSize:         512,
		TenantQueueSize:       32,
		GracefulTimeout:       30 * time.Second,
		TaskTimeout:           2 * time.Minute,
		TenantRefreshInterval: 30 * time.Second,
		RetryPolicy: workerpool.RetryPolicy{
			Attempts: workerpool.AttemptsConfig{
				Count:    3,
				MinDelay: time.Second,
				MaxDelay: 30 * time.Second,
			},
		},
	}

	// NewWorkerManager вызывает cfg.Validate() внутри.
	manager, err := workerpool.NewWorkerManager(workerpool.WorkerManagerParams{
		TenantProvider: myProvider, // реализует workerpool.TenantProvider
		Config:         cfg,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := manager.Start(); err != nil {
		log.Fatal(err)
	}
	defer manager.Stop()

	// Реестр executor'ов: ключи соответствуют полю ExecutorKey в job store.
	registry := workerpool.NewExecutorRegistry()
	registry.MustRegister("sync_orders", &SyncOrdersExecutor{})

	// Разовая задача для конкретного тенанта.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	exec, _ := registry.Get("sync_orders")
	err = manager.SubmitTask(tenantID, workerpool.Task{
		Ctx:      ctx,
		TaskID:   uuid.New(),
		TenantID: tenantID,
		Executor: exec,
		Complete: func(err error) {
			cancel()
			if err != nil {
				slog.Error("task failed", "error", err)
			}
		},
	})
	if err != nil {
		cancel()
		log.Println("submit failed:", err)
	}

# Модель конкурентности

  - Глобальный пул: WorkerCount горутин, разделяемых между всеми тенантами.
  - На тенанта: одна горутина-диспетчер и взвешенный семафор размером
    Tenant.GetWorkerLimit(). Не более Limit задач тенанта выполняются
    в пуле одновременно.
  - Итоговая конкурентность ≤ min(сумма лимитов тенантов, WorkerCount).

# Логирование и трассировка

Логирование ведётся через slog.Default() — настройте глобальный логгер
до вызова NewWorkerManager.

OTel-span создаётся вокруг каждого вызова Executor.Execute. Если Task.Ctx
содержит активный span вызывающего кода (River worker, HTTP-обработчик),
span пула автоматически становится его дочерним. Настройте глобальные
провайдеры через otel.SetTracerProvider и otel.SetMeterProvider до старта
менеджера; при отсутствии настройки используется noop-реализация.

# Корректное завершение

Stop() отменяет контексты обновлятора и всех диспетчеров, ожидает выхода
горутин, затем останавливает пул. Пул сливает очередь в течение
GracefulTimeout; по истечении — принудительно отменяет контексты всех
активных задач.

# Безопасность конкурентного доступа

Все экспортированные методы безопасны для конкурентного использования.
*/
package workerpool
