package workerpool

import "time"

// Config holds all configuration parameters for the worker pool.
// All fields have sensible defaults and can be overridden via YAML or environment variables.
type Config struct {
	// TaskQueueSize is the capacity of the global task queue.
	// If the queue is full, additional tasks will be rejected with an error.
	TaskQueueSize int `yaml:"task_queue_size" env-default:"100"`

	// GracefulTimeout is the maximum duration to wait for running tasks to complete
	// during shutdown. After this timeout, remaining tasks are cancelled.
	GracefulTimeout time.Duration `yaml:"graceful_timeout" env-default:"5m"`

	// TaskTimeout is the maximum execution time for a single task.
	// Tasks exceeding this timeout are cancelled.
	TaskTimeout time.Duration `yaml:"task_timeout" env-default:"5m"`

	// RetryPolicy defines how failed tasks are retried.
	RetryPolicy RetryPolicy `yaml:"retry_policy"`

	// PoolSize defines the number of workers in the global pool for different priority levels.
	PoolSize PoolSize `yaml:"pool_size"`
}

// PoolSize defines worker counts for different pool configurations.
// These can be used to prioritize different types of workloads.
type PoolSize struct {
	// Single worker pool - for sequential operations
	Single int `yaml:"single" env-default:"1"`

	// Low priority pool - for background tasks
	Low int `yaml:"low" env-default:"8"`

	// Normal priority pool - for standard operations
	Normal int `yaml:"normal" env-default:"32"`

	// High priority pool - for time-sensitive operations
	High int `yaml:"high" env-default:"64"`
}

// RetryPolicy defines how task execution retries are handled.
type RetryPolicy struct {
	// Jitter configuration for initial task scheduling
	Jitter JitterConfig `yaml:"jitter"`

	// Attempts configures the retry behavior for failed tasks.
	Attempts AttemptsConfig `yaml:"attempts"`
}

// JitterConfig defines the parameters for random delay before first execution.
type JitterConfig struct {
	// MinDelay is the minimum jitter duration
	MinDelay time.Duration `yaml:"min_delay" env-default:"5s"`

	// MaxDelay is the maximum jitter duration
	MaxDelay time.Duration `yaml:"max_delay" env-default:"10s"`
}

// AttemptsConfig defines the parameters for retry attempts.
type AttemptsConfig struct {
	// Count is the maximum number of execution attempts (including the first).
	// If set to 3, a task will be tried up to 3 times before failing.
	Count int `yaml:"count" env-default:"3"`

	// MinDelay is the initial delay before the first retry.
	MinDelay time.Duration `yaml:"min_delay" env-default:"1s"`

	// MaxDelay is the maximum delay between retries (cap for exponential backoff).
	MaxDelay time.Duration `yaml:"max_delay" env-default:"5s"`
}
