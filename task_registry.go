package workerpool

import "fmt"

type Factory func() Task

type TaskRegistry struct {
	tasks map[string]Factory
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		tasks: make(map[string]Factory),
	}
}

func (r *TaskRegistry) Register(name string, f Factory) {
	r.tasks[name] = f
}

func (r *TaskRegistry) Load(name string) (Task, error) {
	f, ok := r.tasks[name]
	if !ok {
		return nil, fmt.Errorf("task not registered: %s", name)
	}

	return f(), nil
}
