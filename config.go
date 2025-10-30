package workerpool

import "time"

type Config struct {
	TaskQueueSize   int           `yaml:"task_queue_size" env-default:"100"`
	GracefulTimeout time.Duration `yaml:"graceful_timeout" env-default:"5m"`
	WorkerLimit     WorkerLimit   `yaml:"worker_limit"`
	RetryPolicy     RetryPolicy   `yaml:"retry_policy"`
	Size            Size          `yaml:"worker_pool_size"`
	Interval        Interval      `yaml:"interval"`
}

type Interval struct {
	Fast      time.Duration `yaml:"fast" env-default:"30s"`
	Immediate time.Duration `yaml:"immediate" env-default:"100s"`
	Frequent  time.Duration `yaml:"frequent" env-default:"150s"`
	Normal    time.Duration `yaml:"normal" env-default:"1h"`
	Rate      time.Duration `yaml:"rate" env-default:"24h"`
	// специальные интервалы
	MsgDelivery time.Duration `yaml:"msg_delivery" env-default:"60s"`
}

type Size struct {
	Single int `yaml:"single" env-default:"1"`
	Low    int `yaml:"low" env-default:"8"`
	Normal int `yaml:"normal" env-default:"32"`
	Hight  int `yaml:"hight" env-default:"64"`
}

type RetryPolicy struct {
	Jitter   Jitter
	Attempts Attempts
}

type Jitter struct {
	MinDelay time.Duration `yaml:"min_delay" env-default:"5s"`
	MaxDelay time.Duration `yaml:"max_delay" env-default:"10s"`
}

type Attempts struct {
	Count    int           `yaml:"count" env-default:"3"`
	MinDelay time.Duration `yaml:"min_delay" env-default:"1s"`
	MaxDelay time.Duration `yaml:"max_delay" env-default:"5s"`
}

type WorkerLimit struct {
	Event int `yaml:"event" env-default:"0"`
}
