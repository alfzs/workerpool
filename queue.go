package workerpool

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type PoolTask struct {
	TaskName string
	TenantID uuid.UUID
	Priority TaskPriority
	Executor Task
	Ctx      context.Context

	createdAt         time.Time
	effectivePriority float64

	OnComplete func()
}

type priorityHeap struct {
	tasks       []*PoolTask
	agingFactor float64
}

func newPriorityHeap(aging float64) *priorityHeap {
	return &priorityHeap{
		tasks:       make([]*PoolTask, 0),
		agingFactor: aging,
	}
}

func (h priorityHeap) Len() int { return len(h.tasks) }

func (h priorityHeap) Less(i, j int) bool {
	a := h.tasks[i]
	b := h.tasks[j]

	if a.effectivePriority != b.effectivePriority {
		return a.effectivePriority > b.effectivePriority
	}
	return a.createdAt.Before(b.createdAt)
}

func (h priorityHeap) Swap(i, j int) {
	h.tasks[i], h.tasks[j] = h.tasks[j], h.tasks[i]
}

func (h *priorityHeap) Push(x any) {
	h.tasks = append(h.tasks, x.(*PoolTask))
}

func (h *priorityHeap) Pop() any {
	old := h.tasks
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	h.tasks = old[:n-1]
	return x
}

func (h *priorityHeap) compute(task *PoolTask) float64 {
	wait := time.Since(task.createdAt).Seconds()

	var base float64
	switch task.Priority {
	case PriorityHigh:
		base = 300
	case PriorityNormal:
		base = 200
	default:
		base = 100
	}

	return base + wait*h.agingFactor
}
