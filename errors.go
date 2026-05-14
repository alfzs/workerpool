package workerpool

import "fmt"

type PanicError struct {
	TaskName string
	WorkerID int
	Value    any
	Stack    string
}

func (e *PanicError) Error() string {
	return fmt.Sprintf(
		"panic in task=%s worker=%d: %v",
		e.TaskName,
		e.WorkerID,
		e.Value,
	)
}
