package workerpool

import "time"

type Config struct {
	QueueSize       int
	TaskTimeout     time.Duration
	GracefulTimeout time.Duration

	PoolSize struct {
		Workers int
	}

	Retry struct {
		MaxAttempts int
		MinDelay    time.Duration
		MaxDelay    time.Duration
	}
}
