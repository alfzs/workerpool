# workerpool

[![Go Version](https://img.shields.io/github/go-mod/go-version/alfzs/workerpool)](https://go.dev/) [![License](https://img.shields.io/github/license/alfzs/workerpool)](./LICENSE) [![Build Status](https://img.shields.io/github/actions/workflow/status/alfzs/workerpool/test.yml?branch=main)](https://github.com/alfzs/workerpool/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/alfzs/workerpool/v2)](https://goreportcard.com/report/github.com/alfzs/workerpool/v2) [![Go Reference](https://pkg.go.dev/badge/github.com/alfzs/workerpool/v2.svg)](https://pkg.go.dev/github.com/alfzs/workerpool/v2)

Пул воркеров с изоляцией по тенантам: общий глобальный пул горутин, per-tenant лимит конкурентности через взвешенный семафор, повторные попытки с экспоненциальным backoff и jitter, трассировка/метрики через OpenTelemetry и корректный graceful shutdown.

```go
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
```

## 🚀 Быстрый старт

```bash
go get github.com/alfzs/workerpool/v2
```

```go
package main

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/alfzs/workerpool/v2"
)

func main() {
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

	registry := workerpool.NewExecutorRegistry()
	registry.MustRegister("sync_orders", &SyncOrdersExecutor{})

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
}
```

## ✨ Возможности

### Изоляция по тенантам

Каждый тенант получает собственную горутину-диспетчер и взвешенный семафор
размером `Tenant.GetWorkerLimit()`. Задачи тенанта проходят через семафор
диспетчера, прежде чем попасть в общий глобальный пул — итоговая
конкурентность ограничена `min(сумма лимитов тенантов, Config.WorkerCount)`.
Список активных тенантов периодически обновляется через `TenantProvider`
(интервал — `Config.TenantRefreshInterval`), а изменение лимита воркеров
применяется на следующем цикле обновления без прерывания уже выполняющихся
задач.

### Общий глобальный пул

`Config.WorkerCount` горутин обслуживают задачи всех тенантов одновременно.
Задачи, поступающие при заполненной очереди (`Config.TaskQueueSize`),
отклоняются немедленно — `SubmitTask` и внутренняя передача в пул
неблокирующие.

### Повторные попытки с backoff и jitter

`Config.RetryPolicy` управляет количеством попыток и экспоненциальным
backoff между ними с полным jitter (равномерное случайное значение в
`[0, cap]`) для предотвращения thundering herd при массовых сбоях. При
использовании [River](https://github.com/riverqueue/river) как job store
установите `RetryPolicy.Attempts.Count = 1`, чтобы не дублировать ретраи
River.

### Реестр executor'ов

`ExecutorRegistry` сопоставляет строковые ключи (хранящиеся, например, в
job store) с реализациями `TaskExecutor` — задание в базе хранит только
ключ, а конкретная реализация разрешается во время исполнения.

### Трассировка и метрики через OpenTelemetry

Вокруг каждого вызова `Executor.Execute` создаётся OTel-span. Если
`Task.Ctx` уже содержит активный span вызывающего кода (River worker,
HTTP-обработчик), span пула автоматически становится его дочерним.
Настройте глобальные провайдеры через `otel.SetTracerProvider` и
`otel.SetMeterProvider` до старта менеджера; без настройки используется
noop-реализация без накладных расходов.

### Graceful shutdown

`Stop()` отменяет контексты обновлятора и всех диспетчеров, дожидается
выхода горутин, затем останавливает пул. Пул сливает очередь в течение
`Config.GracefulTimeout`; по истечении таймаута контексты всех активных
задач отменяются принудительно.

### Health-снимки

`WorkerManager.Health()` возвращает снимок состояния (глубина очередей,
количество тенантов, лимиты) для liveness/readiness-проб и дашбордов.

Полная схема архитектуры и интеграции с River — в [ARCHITECTURE.md](ARCHITECTURE.md).

## 🤝 Contributing

Правила участия — в [CONTRIBUTING.md](CONTRIBUTING.md).

## 📄 License

Проект распространяется по лицензии MIT — см. [LICENSE](LICENSE).
