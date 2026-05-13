package workerpool

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	TasksTotal        uint64
	TasksFailedTotal  uint64
	TasksRetryTotal   uint64
	TasksDroppedTotal uint64
	WorkerPanicsTotal uint64

	ActiveWorkers int64
	QueuedTasks   int64
	ActiveTenants int64

	TaskDurationSum   uint64
	TaskDurationCount uint64
	RetryDelaySum     uint64
	RetryDelayCount   uint64
}

var globalMetrics Metrics

func IncTasksTotal()        { atomic.AddUint64(&globalMetrics.TasksTotal, 1) }
func IncTasksFailedTotal()  { atomic.AddUint64(&globalMetrics.TasksFailedTotal, 1) }
func IncTasksRetryTotal()   { atomic.AddUint64(&globalMetrics.TasksRetryTotal, 1) }
func IncTasksDroppedTotal() { atomic.AddUint64(&globalMetrics.TasksDroppedTotal, 1) }
func IncWorkerPanicsTotal() { atomic.AddUint64(&globalMetrics.WorkerPanicsTotal, 1) }

func SetActiveWorkers(v int64) { atomic.StoreInt64(&globalMetrics.ActiveWorkers, v) }
func SetQueuedTasks(v int64)   { atomic.StoreInt64(&globalMetrics.QueuedTasks, v) }
func SetActiveTenants(v int64) { atomic.StoreInt64(&globalMetrics.ActiveTenants, v) }

func ObserveTaskDuration(d time.Duration) {
	atomic.AddUint64(&globalMetrics.TaskDurationSum, uint64(d))
	atomic.AddUint64(&globalMetrics.TaskDurationCount, 1)
}

func ObserveRetryDelay(d time.Duration) {
	atomic.AddUint64(&globalMetrics.RetryDelaySum, uint64(d))
	atomic.AddUint64(&globalMetrics.RetryDelayCount, 1)
}

func GetMetrics() Metrics {
	return Metrics{
		TasksTotal:        atomic.LoadUint64(&globalMetrics.TasksTotal),
		TasksFailedTotal:  atomic.LoadUint64(&globalMetrics.TasksFailedTotal),
		TasksRetryTotal:   atomic.LoadUint64(&globalMetrics.TasksRetryTotal),
		TasksDroppedTotal: atomic.LoadUint64(&globalMetrics.TasksDroppedTotal),
		WorkerPanicsTotal: atomic.LoadUint64(&globalMetrics.WorkerPanicsTotal),
		ActiveWorkers:     atomic.LoadInt64(&globalMetrics.ActiveWorkers),
		QueuedTasks:       atomic.LoadInt64(&globalMetrics.QueuedTasks),
		ActiveTenants:     atomic.LoadInt64(&globalMetrics.ActiveTenants),
	}
}
