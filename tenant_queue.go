package workerpool

import "container/list"

type tenantQueue struct {
	tenantID string

	quantum int64
	deficit int64

	queue *list.List

	active bool
}

func newTenantQueue(tenantID string, quantum int64) *tenantQueue {
	return &tenantQueue{
		tenantID: tenantID,
		quantum:  quantum,
		queue:    list.New(),
	}
}

func (q *tenantQueue) push(task *PoolTask) {
	q.queue.PushBack(task)
}

func (q *tenantQueue) pop() *PoolTask {
	elem := q.queue.Front()
	if elem == nil {
		return nil
	}

	q.queue.Remove(elem)

	return elem.Value.(*PoolTask)
}

func (q *tenantQueue) len() int {
	return q.queue.Len()
}
