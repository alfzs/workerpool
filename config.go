package workerpool

import (
	"time"

	"github.com/google/uuid"
)

type TenantQuantumResolver func(tenantID uuid.UUID) int64

type Config struct {
	Workers         int
	QueueSize       int
	GracefulTimeout time.Duration
	TaskTimeout     time.Duration

	Retry RetryConfig

	// tenant quotas
	DefaultQuantum int64
	MaxTenantQueue int

	// optional per-tenant quantum override
	TenantQuantumResolver TenantQuantumResolver
}

type RetryConfig struct {
	MaxAttempts int
	MinDelay    time.Duration
	MaxDelay    time.Duration
}
