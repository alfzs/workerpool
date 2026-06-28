package workerpool

import (
	"errors"
	"fmt"
	"time"
)

// Config содержит все настраиваемые параметры пула воркеров.
// Нулевые значения недопустимы — всегда вызывайте Validate() перед
// передачей конфига в NewWorkerManager.
type Config struct {
	// WorkerCount — количество горутин в глобальном пуле исполнения.
	// Все тенанты разделяют эту ёмкость. Должно быть > 0.
	WorkerCount int `yaml:"worker_count" env-default:"256"`

	// TaskQueueSize — ёмкость канала глобального пула.
	// Задачи, поступающие при заполненном канале, отклоняются немедленно.
	// Должно быть > 0.
	TaskQueueSize int `yaml:"task_queue_size" env-default:"512"`

	// TenantQueueSize — ёмкость буфера задач каждого тенанта.
	// Устанавливается однократно при создании тенанта; не изменяется
	// при обновлении лимита воркеров. Должно быть > 0.
	TenantQueueSize int `yaml:"tenant_queue_size" env-default:"64"`

	// GracefulTimeout — максимальное время ожидания завершения активных задач
	// при вызове Stop(). По истечении таймаута все активные контексты задач
	// принудительно отменяются. Должно быть > 0.
	GracefulTimeout time.Duration `yaml:"graceful_timeout" env-default:"30s"`

	// TaskTimeout — дедлайн по умолчанию для одного выполнения задачи.
	// Вызывающий код может задать более жёсткий дедлайн через Task.Ctx.
	// Должно быть > 0.
	TaskTimeout time.Duration `yaml:"task_timeout" env-default:"5m"`

	// TenantRefreshInterval — как часто менеджер перезапрашивает список
	// активных тенантов у TenantProvider. Должно быть > 0.
	TenantRefreshInterval time.Duration `yaml:"tenant_refresh_interval" env-default:"30s"`

	// RetryPolicy задаёт поведение при повторных попытках выполнения задачи.
	// При использовании River как job store River управляет внешними ретраями;
	// в этом случае установите Attempts.Count = 1.
	RetryPolicy RetryPolicy `yaml:"retry_policy"`
}

// RetryPolicy определяет поведение при повторных попытках внутри пула.
type RetryPolicy struct {
	// Jitter — случайная задержка перед самым первым выполнением задачи,
	// используемая для размазывания нагрузки по кластеру.
	Jitter JitterConfig `yaml:"jitter"`

	// Attempts — параметры повторных попыток при ошибках.
	Attempts AttemptsConfig `yaml:"attempts"`
}

// JitterConfig определяет границы случайной задержки перед первым запуском.
type JitterConfig struct {
	// MinDelay — минимальная задержка jitter. Должно быть >= 0.
	MinDelay time.Duration `yaml:"min_delay" env-default:"0s"`

	// MaxDelay — максимальная задержка jitter. Должно быть >= MinDelay.
	MaxDelay time.Duration `yaml:"max_delay" env-default:"5s"`
}

// AttemptsConfig определяет параметры повторных попыток.
type AttemptsConfig struct {
	// Count — суммарное количество попыток, включая первую.
	// При использовании River установите 1, чтобы не дублировать ретраи.
	// Должно быть >= 1.
	Count int `yaml:"count" env-default:"3"`

	// MinDelay — начальная пауза перед первой повторной попыткой.
	MinDelay time.Duration `yaml:"min_delay" env-default:"1s"`

	// MaxDelay — верхняя граница экспоненциального backoff.
	// Должно быть >= MinDelay.
	MaxDelay time.Duration `yaml:"max_delay" env-default:"30s"`
}

// Validate проверяет корректность всех обязательных полей.
// Возвращает объединённую ошибку со всеми нарушениями сразу, чтобы
// вызывающий код мог вывести полный список проблем в одном сообщении.
func (c Config) Validate() error {
	var errs []error

	if c.WorkerCount <= 0 {
		errs = append(errs, fmt.Errorf("WorkerCount must be > 0, got %d", c.WorkerCount))
	}
	if c.TaskQueueSize <= 0 {
		errs = append(errs, fmt.Errorf("TaskQueueSize must be > 0, got %d", c.TaskQueueSize))
	}
	if c.TenantQueueSize <= 0 {
		errs = append(errs, fmt.Errorf("TenantQueueSize must be > 0, got %d", c.TenantQueueSize))
	}
	if c.GracefulTimeout <= 0 {
		errs = append(errs, fmt.Errorf("GracefulTimeout must be > 0, got %v", c.GracefulTimeout))
	}
	if c.TaskTimeout <= 0 {
		errs = append(errs, fmt.Errorf("TaskTimeout must be > 0, got %v", c.TaskTimeout))
	}
	if c.TenantRefreshInterval <= 0 {
		errs = append(errs, fmt.Errorf("TenantRefreshInterval must be > 0, got %v", c.TenantRefreshInterval))
	}
	if c.RetryPolicy.Attempts.Count <= 0 {
		errs = append(errs, fmt.Errorf("RetryPolicy.Attempts.Count must be >= 1, got %d", c.RetryPolicy.Attempts.Count))
	}
	if c.RetryPolicy.Attempts.MaxDelay < c.RetryPolicy.Attempts.MinDelay {
		errs = append(errs, fmt.Errorf("RetryPolicy.Attempts.MaxDelay must be >= MinDelay"))
	}
	if c.RetryPolicy.Jitter.MaxDelay < c.RetryPolicy.Jitter.MinDelay {
		errs = append(errs, fmt.Errorf("RetryPolicy.Jitter.MaxDelay must be >= MinDelay"))
	}

	return errors.Join(errs...)
}
