package workerpool

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
)

// newTestPool создаёт пул для тестов уровня pool (без WorkerManager).
func newTestPool(tb testing.TB, cfg Config) *pool {
	tb.Helper()

	p, err := newPool(poolParams{config: cfg, logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		tb.Fatalf("newPool: %v", err)
	}

	return p
}

// TestPoolAddTaskAfterStop проверяет, что addTask после stop() немедленно
// возвращает ErrPoolStopping, а не пытается отправить задачу в закрытый канал.
func TestPoolAddTaskAfterStop(t *testing.T) {
	t.Parallel()

	p := newTestPool(t, newTestConfig())
	p.start()
	p.stop()

	err := p.addTask(newTask(uuid.New(), successExec(), func(error) {}))
	if !errors.Is(err, ErrPoolStopping) {
		t.Errorf("addTask after stop: err = %v, want errors.Is(err, ErrPoolStopping)", err)
	}
}

// TestPoolAddTaskQueueFull проверяет, что addTask возвращает ErrQueueFull,
// когда taskChan заполнен и ни один воркер не готов его вычитать —
// неблокирующая отправка обязана провалиться немедленно, а не зависнуть.
func TestPoolAddTaskQueueFull(t *testing.T) {
	t.Parallel()

	cfg := newTestConfig()
	cfg.TaskQueueSize = 1
	cfg.WorkerCount = 1

	p := newTestPool(t, cfg)
	p.start()
	t.Cleanup(p.stop)

	blockRelease := make(chan struct{})
	defer close(blockRelease)

	blocker := &mockExecutor{fn: func(_ context.Context, _ uuid.UUID, _ int) error {
		<-blockRelease
		return nil
	}}

	// Занимает единственного воркера — taskChan снова пуст сразу после того,
	// как воркер вычитает эту задачу.
	if err := p.addTask(newTask(uuid.New(), blocker, func(error) {})); err != nil {
		t.Fatalf("addTask blocker: %v", err)
	}

	time.Sleep(30 * time.Millisecond) // дать воркеру вычитать blocker из taskChan

	// Заполняет единственный слот буфера (cap=1); воркер занят blocker'ом.
	if err := p.addTask(newTask(uuid.New(), successExec(), func(error) {})); err != nil {
		t.Fatalf("addTask filler: %v", err)
	}

	err := p.addTask(newTask(uuid.New(), successExec(), func(error) {}))
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("addTask on full queue: err = %v, want errors.Is(err, ErrQueueFull)", err)
	}
}
