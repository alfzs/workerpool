package workerpool

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	valid := Config{
		WorkerCount:           1,
		TaskQueueSize:         1,
		TenantQueueSize:       1,
		GracefulTimeout:       time.Second,
		TaskTimeout:           time.Second,
		TenantRefreshInterval: time.Second,
		RetryPolicy: RetryPolicy{
			Attempts: AttemptsConfig{Count: 1},
		},
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"WorkerCount=0", func(c *Config) { c.WorkerCount = 0 }, true},
		{"WorkerCount negative", func(c *Config) { c.WorkerCount = -1 }, true},
		{"TaskQueueSize=0", func(c *Config) { c.TaskQueueSize = 0 }, true},
		{"TenantQueueSize=0", func(c *Config) { c.TenantQueueSize = 0 }, true},
		{"GracefulTimeout=0", func(c *Config) { c.GracefulTimeout = 0 }, true},
		{"TaskTimeout=0", func(c *Config) { c.TaskTimeout = 0 }, true},
		{"TenantRefreshInterval=0", func(c *Config) { c.TenantRefreshInterval = 0 }, true},
		{"Attempts.Count=0", func(c *Config) { c.RetryPolicy.Attempts.Count = 0 }, true},
		{"Attempts.Count negative", func(c *Config) { c.RetryPolicy.Attempts.Count = -1 }, true},
		{"Attempts.MaxDelay < MinDelay", func(c *Config) {
			c.RetryPolicy.Attempts.MinDelay = time.Second
			c.RetryPolicy.Attempts.MaxDelay = time.Millisecond
		}, true},
		{"Jitter.MaxDelay < MinDelay", func(c *Config) {
			c.RetryPolicy.Jitter.MinDelay = time.Second
			c.RetryPolicy.Jitter.MaxDelay = time.Millisecond
		}, true},
		{"multiple violations joined", func(c *Config) {
			c.WorkerCount = 0
			c.TaskQueueSize = 0
			c.GracefulTimeout = 0
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			tt.mutate(&cfg)

			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}

			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
