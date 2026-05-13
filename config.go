package workerpool

import "time"

// Config содержит все параметры конфигурации воркер-пула.
type Config struct {
	// TaskQueueSize — ёмкость глобальной очереди задач.
	// При заполнении новые задачи отклоняются с ошибкой.
	TaskQueueSize int `yaml:"task_queue_size" env-default:"100"`

	// GracefulTimeout — максимальное время ожидания завершения задач при остановке.
	GracefulTimeout time.Duration `yaml:"graceful_timeout" env-default:"5m"`

	// TaskTimeout — максимальное время выполнения одной задачи.
	TaskTimeout time.Duration `yaml:"task_timeout" env-default:"5m"`

	// RetryPolicy — политика повторных попыток для упавших задач.
	RetryPolicy RetryPolicy `yaml:"retry_policy"`

	// PoolSize — количество воркеров в глобальном пуле.
	PoolSize PoolSize `yaml:"pool_size"`
}

// PoolSize определяет количество воркеров для разных приоритетов.
type PoolSize struct {
	// Single — для последовательных операций
	Single int `yaml:"single" env-default:"1"`

	// Low — для фоновых задач
	Low int `yaml:"low" env-default:"8"`

	// Normal — для стандартных операций
	Normal int `yaml:"normal" env-default:"32"`

	// High — для чувствительных ко времени операций
	High int `yaml:"high" env-default:"64"`
}

// RetryPolicy определяет поведение повторных попыток.
type RetryPolicy struct {
	Attempts AttemptsConfig `yaml:"attempts"`
}

// AttemptsConfig параметры повторных попыток.
type AttemptsConfig struct {
	// Count — максимальное количество попыток (включая первую)
	Count int `yaml:"count" env-default:"3"`

	// MinDelay — начальная задержка перед первой ретраем
	MinDelay time.Duration `yaml:"min_delay" env-default:"1s"`

	// MaxDelay — максимальная задержка между ретраями
	MaxDelay time.Duration `yaml:"max_delay" env-default:"5s"`
}
