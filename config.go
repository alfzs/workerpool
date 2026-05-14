package workerpool

import "time"

type Config struct {
	Workers         int
	QueueSize       int
	GracefulTimeout time.Duration
	TaskTimeout     time.Duration

	Retry RetryConfig

	// tenant quotas
	DefaultQuantum int64
	MaxTenantQueue int
}

type RetryConfig struct {
	MaxAttempts int
	MinDelay    time.Duration
	MaxDelay    time.Duration
}
