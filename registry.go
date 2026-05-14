package workerpool

import "fmt"

type Factory func() Task

type Registry struct {
	tasks map[string]Factory
}

func NewRegistry() *Registry {
	return &Registry{
		tasks: make(map[string]Factory),
	}
}

func (r *Registry) Register(name string, f Factory) {
	r.tasks[name] = f
}

func (r *Registry) Create(name string) (Task, error) {
	f, ok := r.tasks[name]
	if !ok {
		return nil, fmt.Errorf(
			"task not registered: %s",
			name,
		)
	}

	return f(), nil
}
