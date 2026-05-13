package workerpool

import "time"

// Config содержит все параметры конфигурации воркер-пула.
type Config struct {
	// Размеры очередей для разных приоритетов.
	HighPriorityQueueSize   int `yaml:"high_priority_queue_size" env-default:"500"`
	NormalPriorityQueueSize int `yaml:"normal_priority_queue_size" env-default:"2000"`
	LowPriorityQueueSize    int `yaml:"low_priority_queue_size" env-default:"5000"`

	// GracefulTimeout — максимальное время ожидания завершения задач при остановке.
	GracefulTimeout time.Duration `yaml:"graceful_timeout" env-default:"5m"`

	// TaskTimeout — максимальное время выполнения одной задачи.
	TaskTimeout time.Duration `yaml:"task_timeout" env-default:"5m"`

	// CronTaskTimeout — максимальное время выполнения cron задачи.
	CronTaskTimeout time.Duration `yaml:"cron_task_timeout" env-default:"10m"`

	// RetryPolicy — политика повторных попыток для упавших задач.
	RetryPolicy RetryPolicy `yaml:"retry_policy"`

	// PoolSize — количество воркеров для каждого приоритета.
	PoolSize PoolSize `yaml:"pool_size"`

	// CronCheckInterval — интервал проверки cron-задач.
	CronCheckInterval time.Duration `yaml:"cron_check_interval" env-default:"1m"`
}

// PoolSize определяет количество воркеров для разных приоритетов.
type PoolSize struct {
	High   int `yaml:"high" env-default:"64"`
	Normal int `yaml:"normal" env-default:"32"`
	Low    int `yaml:"low" env-default:"8"`
}

// RetryPolicy определяет поведение повторных попыток.
type RetryPolicy struct {
	Attempts AttemptsConfig `yaml:"attempts"`
}

// AttemptsConfig параметры повторных попыток.
type AttemptsConfig struct {
	Count    int           `yaml:"count" env-default:"3"`
	MinDelay time.Duration `yaml:"min_delay" env-default:"1s"`
	MaxDelay time.Duration `yaml:"max_delay" env-default:"5s"`
}
