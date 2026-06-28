package workerpool

import (
	"fmt"
	"sync"
)

// ExecutorRegistry сопоставляет строковые ключи с реализациями TaskExecutor.
// Служит мостом между job store (например, River/Postgres) и пулом: задание,
// сохранённое в базе, хранит только строковый ключ executor'а; реестр
// разрешает его в конкретную реализацию во время исполнения.
//
// Все методы безопасны для конкурентного использования.
type ExecutorRegistry struct {
	mu        sync.RWMutex
	executors map[string]TaskExecutor
}

// NewExecutorRegistry создаёт пустой реестр.
func NewExecutorRegistry() *ExecutorRegistry {
	return &ExecutorRegistry{
		executors: make(map[string]TaskExecutor),
	}
}

// Register регистрирует executor под заданным ключом.
// Возвращает ошибку, если ключ пустой, executor равен nil или ключ уже
// зарегистрирован. Ключи чувствительны к регистру.
func (r *ExecutorRegistry) Register(key string, exec TaskExecutor) error {
	if key == "" {
		return fmt.Errorf("executor key cannot be empty")
	}
	if exec == nil {
		return fmt.Errorf("executor for key %q is nil", key)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.executors[key]; exists {
		return fmt.Errorf("executor %q already registered", key)
	}

	r.executors[key] = exec
	return nil
}

// MustRegister аналогичен Register, но паникует при ошибке.
// Предназначен для использования в init() или TestMain, где ошибка
// регистрации является программной ошибкой и не должна возникать в рантайме.
func (r *ExecutorRegistry) MustRegister(key string, exec TaskExecutor) {
	if err := r.Register(key, exec); err != nil {
		panic(fmt.Sprintf("workerpool: MustRegister %q: %v", key, err))
	}
}

// Get возвращает executor, зарегистрированный под ключом key.
// Возвращает ошибку, если ключ не найден.
func (r *ExecutorRegistry) Get(key string) (TaskExecutor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	exec, ok := r.executors[key]
	if !ok {
		return nil, fmt.Errorf("executor %q not found", key)
	}
	return exec, nil
}

// Keys возвращает снимок всех зарегистрированных ключей в произвольном порядке.
func (r *ExecutorRegistry) Keys() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	keys := make([]string, 0, len(r.executors))
	for k := range r.executors {
		keys = append(keys, k)
	}
	return keys
}
