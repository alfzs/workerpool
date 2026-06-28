package workerpool

import (
	"fmt"
	"sync"
	"testing"
)

func TestExecutorRegistry_Register(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		r := NewExecutorRegistry()
		if err := r.Register("key", successExec()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("empty key", func(t *testing.T) {
		t.Parallel()
		r := NewExecutorRegistry()
		if err := r.Register("", successExec()); err == nil {
			t.Error("expected error for empty key")
		}
	})

	t.Run("nil executor", func(t *testing.T) {
		t.Parallel()
		r := NewExecutorRegistry()
		if err := r.Register("key", nil); err == nil {
			t.Error("expected error for nil executor")
		}
	})

	t.Run("duplicate key", func(t *testing.T) {
		t.Parallel()
		r := NewExecutorRegistry()
		_ = r.Register("key", successExec())
		if err := r.Register("key", successExec()); err == nil {
			t.Error("expected error for duplicate key")
		}
	})
}

func TestExecutorRegistry_Get(t *testing.T) {
	t.Parallel()

	t.Run("found", func(t *testing.T) {
		t.Parallel()
		r := NewExecutorRegistry()
		exec := successExec()
		_ = r.Register("key", exec)

		got, err := r.Get("key")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != exec {
			t.Error("returned wrong executor instance")
		}
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		r := NewExecutorRegistry()
		if _, err := r.Get("missing"); err == nil {
			t.Error("expected error for missing key")
		}
	})
}

func TestExecutorRegistry_MustRegister_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate MustRegister, got none")
		}
	}()
	r := NewExecutorRegistry()
	r.MustRegister("key", successExec())
	r.MustRegister("key", successExec()) // дубликат — должна быть паника
}

func TestExecutorRegistry_Keys(t *testing.T) {
	t.Parallel()

	r := NewExecutorRegistry()
	want := []string{"alpha", "beta", "gamma"}
	for _, k := range want {
		_ = r.Register(k, successExec())
	}

	keys := r.Keys()
	if len(keys) != len(want) {
		t.Fatalf("expected %d keys, got %d", len(want), len(keys))
	}
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	for _, k := range want {
		if !keySet[k] {
			t.Errorf("key %q missing from Keys()", k)
		}
	}
}

// TestExecutorRegistry_ConcurrentAccess проверяет отсутствие гонок при
// параллельных Register/Get. Запускать с флагом -race.
func TestExecutorRegistry_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	r := NewExecutorRegistry()
	_ = r.Register("seed", successExec())

	const n = 64
	var wg sync.WaitGroup
	wg.Add(n * 2)

	for i := range n {
		go func(i int) {
			defer wg.Done()
			_ = r.Register(fmt.Sprintf("key-%d", i), successExec())
		}(i)
		go func() {
			defer wg.Done()
			_, _ = r.Get("seed")
		}()
	}
	wg.Wait()
}
