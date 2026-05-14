package workerpool

import (
	"container/list"
	"fmt"

	"github.com/google/uuid"
)

type drrScheduler struct {
	tenants map[string]*tenantQueue

	active *list.List

	defaultQuantum int64
	maxTenantQueue int

	tenantQuantumResolver TenantQuantumResolver
}

func newDRRScheduler(
	defaultQuantum int64,
	maxTenantQueue int,
	resolver TenantQuantumResolver,
) *drrScheduler {

	return &drrScheduler{
		tenants:               make(map[string]*tenantQueue),
		active:                list.New(),
		defaultQuantum:        defaultQuantum,
		maxTenantQueue:        maxTenantQueue,
		tenantQuantumResolver: resolver,
	}
}

func (s *drrScheduler) quantumForTenant(
	tenantID uuid.UUID,
) int64 {

	if s.tenantQuantumResolver != nil {
		if q := s.tenantQuantumResolver(tenantID); q > 0 {
			return q
		}
	}

	return s.defaultQuantum
}

func (s *drrScheduler) enqueue(task *PoolTask) error {
	tenantID := task.TenantID.String()

	q, exists := s.tenants[tenantID]
	if !exists {
		q = newTenantQueue(
			tenantID,
			s.quantumForTenant(task.TenantID),
		)

		s.tenants[tenantID] = q
	}

	if q.len() >= s.maxTenantQueue {
		return fmt.Errorf(
			"tenant queue full tenant=%s",
			tenantID,
		)
	}

	// live quantum refresh
	if updatedQuantum := s.quantumForTenant(task.TenantID); updatedQuantum > 0 {
		q.quantum = updatedQuantum
	}

	q.push(task)

	if !q.active {
		q.active = true
		s.active.PushBack(q)
	}

	return nil
}

func (s *drrScheduler) dequeue() *PoolTask {
	for s.active.Len() > 0 {
		elem := s.active.Front()

		q := elem.Value.(*tenantQueue)

		s.active.Remove(elem)

		if q.len() == 0 {
			q.active = false
			continue
		}

		q.deficit += q.quantum

		if q.deficit <= 0 {
			s.active.PushBack(q)
			continue
		}

		task := q.pop()

		if task == nil {
			q.active = false
			continue
		}

		// fixed task cost = 1
		q.deficit -= 1

		if q.len() > 0 {
			s.active.PushBack(q)
		} else {
			q.active = false
		}

		return task
	}

	return nil
}
