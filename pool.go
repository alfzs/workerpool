package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/google/uuid"
)

// Task — единица работы, передаваемая в пул.
// Поля Ctx, TaskID, TenantID, Executor заполняются вызывающим кодом до Submit;
// Complete оборачивается внутри диспетчера и не должен изменяться после
// передачи задачи.
type Task struct {
	// Ctx — контекст задачи с дедлайном и сигналом отмены.
	// Если Ctx содержит активный OTel-span (например, из River worker или
	// HTTP-обработчика), span пула станет его дочерним — иерархия
	// трассировки выстраивается автоматически через контекст.
	Ctx context.Context //nolint:containedctx // Task is a data envelope passed by value through channels, not a long-lived object; ctx must travel with it

	// TaskID — уникальный идентификатор конкретного запуска
	// (например, ID job в River). Используется в логах и атрибутах span.
	TaskID uuid.UUID

	// TenantID — идентификатор тенанта; используется для маршрутизации,
	// логирования и атрибутов метрик.
	TenantID uuid.UUID

	// ExecutorKey — строковый ключ для разрешения executor'а через
	// ExecutorRegistry. Заполняется, если executor хранится в реестре
	// (типичный случай при использовании River как job store).
	ExecutorKey string

	// Executor — реализация, вызываемая напрямую. Имеет приоритет над
	// ExecutorKey. Удобна для разовых задач без регистрации в реестре.
	Executor TaskExecutor

	// Complete вызывается ровно один раз после завершения задачи —
	// успешного, после исчерпания повторных попыток, после паники или
	// при остановке диспетчера. err == nil означает успех.
	Complete func(err error)
}

// pool — разделяемый пул фиксированного размера, исполняющий задачи всех
// тенантов. Управляет горутинами-воркерами, повторными попытками и OTel.
type pool struct {
	// logger — переданный через poolParams.logger, с добавленным атрибутом
	// component=pool (см. WorkerManagerParams.Logger — единый инжектируемый
	// логгер для менеджера и пула, а не независимый slog.Default() у каждого).
	logger *slog.Logger

	config      Config
	taskChan    chan Task
	workerCount int
	maxAttempts int

	// closeMu защищает инвариант «отправка в taskChan не происходит после его
	// закрытия»: addTask удерживает RLock при проверке isStopping и отправке;
	// stop() удерживает Lock при закрытии канала.
	closeMu sync.RWMutex

	wg         sync.WaitGroup
	isStopping atomic.Bool

	// forceCtx отменяется по истечении GracefulTimeout, распространяя
	// отмену во все активные контексты задач.
	forceCtx    context.Context //nolint:containedctx // lifecycle context for the pool's own graceful-shutdown deadline, not a per-call context
	forceCancel context.CancelFunc

	// OTel-инструменты; инициализируются из глобального провайдера.
	// Если приложение не настроило провайдер — используется noop.
	tracer       trace.Tracer
	taskDuration metric.Float64Histogram
	tasksTotal   metric.Int64Counter
}

// poolParams — внутренние параметры для создания пула.
type poolParams struct {
	config Config
	logger *slog.Logger
}

// newPool создаёт пул. Конфигурация должна быть провалидирована до вызова.
func newPool(p poolParams) (*pool, error) {
	maxAttempts := p.config.RetryPolicy.Attempts.Count
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	forceCtx, forceCancel := context.WithCancel(context.Background())

	// Используем глобальные OTel-провайдеры. Если приложение не настроило их,
	// возвращается noop-реализация без накладных расходов.
	tracer := otel.GetTracerProvider().Tracer("workerpool")
	meter := otel.GetMeterProvider().Meter("workerpool")

	dur, err := meter.Float64Histogram(
		"workerpool.task.duration",
		metric.WithDescription("task execution duration in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		forceCancel()
		return nil, fmt.Errorf("create duration histogram: %w", err)
	}

	total, err := meter.Int64Counter(
		"workerpool.tasks.total",
		metric.WithDescription("total tasks completed, partitioned by status"),
	)
	if err != nil {
		forceCancel()
		return nil, fmt.Errorf("create tasks counter: %w", err)
	}

	return &pool{
		logger:       p.logger.With(slog.String("component", "pool")),
		config:       p.config,
		workerCount:  p.config.WorkerCount,
		maxAttempts:  maxAttempts,
		taskChan:     make(chan Task, p.config.TaskQueueSize),
		forceCtx:     forceCtx,
		forceCancel:  forceCancel,
		tracer:       tracer,
		taskDuration: dur,
		tasksTotal:   total,
	}, nil
}

// start запускает горутины-воркеры. Должен вызываться до первого addTask.
func (p *pool) start() {
	for workerID := range p.workerCount {
		p.wg.Go(func() { p.runWorker(workerID) })
	}
}

// runWorker — цикл одной горутины-воркера. После паники автоматически
// перезапускается, пока пул не переходит в состояние остановки.
// Нормальный выход из worker() означает, что taskChan закрыт и опустошён.
func (p *pool) runWorker(id int) {
	for {
		panicked := true

		func() {
			defer func() {
				if r := recover(); r != nil {
					p.logger.Error("worker panic",
						slog.Int("worker_id", id),
						slog.Any("panic", r),
						slog.String("stack", string(debug.Stack())))
				}
			}()

			p.worker(id)

			panicked = false
		}()

		if !panicked || p.isStopping.Load() {
			return
		}
	}
}

// stop закрывает taskChan и ожидает завершения воркеров.
// При истечении GracefulTimeout вызывает forceCancel и блокируется
// до выхода всех горутин.
func (p *pool) stop() {
	if !p.isStopping.CompareAndSwap(false, true) {
		return
	}

	p.closeMu.Lock()
	close(p.taskChan)
	p.closeMu.Unlock()

	done := make(chan struct{})

	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("pool stopped gracefully")
	case <-time.After(p.config.GracefulTimeout):
		p.logger.Error("graceful timeout exceeded, forcing shutdown")
		p.forceCancel()
		<-done
	}
}

// addTask помещает задачу в очередь пула. Возвращает ошибку немедленно,
// если пул останавливается или очередь заполнена (неблокирующий вызов).
func (p *pool) addTask(task Task) error {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()

	if p.isStopping.Load() {
		return ErrPoolStopping
	}

	select {
	case p.taskChan <- task:
		return nil
	default:
		return fmt.Errorf("%w: capacity %d", ErrQueueFull, cap(p.taskChan))
	}
}

// worker — внутренний цикл одного воркера: читает задачи из taskChan
// до его закрытия.
func (p *pool) worker(id int) {
	for task := range p.taskChan {
		p.runTask(task, id)
	}
}

// runTask исполняет одну задачу с восстановлением после паники и гарантирует
// единственный вызов Complete независимо от результата.
//
// Правило единственной обработки: результат либо передаётся вызывающему коду
// через Complete, либо (если Complete не задан) логируется здесь как
// последняя инстанция — никогда и то, и другое одновременно. Исключение —
// сам факт паники: её стек-трейс логируется безусловно, поскольку это
// информация о дефекте в Executor, недоступная вызывающему коду через
// возвращаемую ошибку.
func (p *pool) runTask(task Task, workerID int) {
	var taskErr error

	defer func() {
		if r := recover(); r != nil {
			taskErr = fmt.Errorf("panic: %v", r)
			p.logger.Error("task panic",
				slog.String("tenant_id", task.TenantID.String()),
				slog.String("task_id", task.TaskID.String()),
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())))
		}

		if task.Complete != nil {
			task.Complete(taskErr)
			return
		}

		if taskErr != nil {
			p.logger.Error("task failed with no completion handler",
				slog.String("tenant_id", task.TenantID.String()),
				slog.String("task_id", task.TaskID.String()),
				slog.Any("error", taskErr))
		}
	}()

	taskErr = p.executeWithRetry(task, workerID)
}

// executeWithRetry запускает Executor с повторными попытками и экспоненциальным
// backoff. Контекст задачи объединяется с forceCtx пула: принудительная
// остановка прерывает как паузу между попытками, так и сам вызов Execute
// (при условии, что Executor соблюдает отмену контекста).
//
// OTel-span создаётся как дочерний относительно span'а, уже находящегося
// в Task.Ctx. Если вызывающий код передал контекст с активным span'ом,
// иерархия трассировки выстраивается автоматически.
func (p *pool) executeWithRetry(task Task, workerID int) error {
	// Создаём дочерний контекст, чтобы forceCancel не изменял контекст
	// вызывающего кода.
	taskCtx, taskCancel := context.WithCancel(task.Ctx)
	defer taskCancel()

	stopForce := context.AfterFunc(p.forceCtx, taskCancel)
	defer stopForce()

	// Span становится дочерним относительно span'а из task.Ctx (если он есть).
	ctx, span := p.tracer.Start(taskCtx, "workerpool.task.execute",
		trace.WithAttributes(
			attribute.String("tenant.id", task.TenantID.String()),
			attribute.String("task.id", task.TaskID.String()),
			attribute.Int("worker.id", workerID),
		))
	defer span.End()

	start := time.Now()

	var lastErr error

	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		span.SetAttributes(attribute.Int("attempt", attempt))

		err := task.Executor.Execute(ctx, task.TenantID, workerID)
		if err == nil {
			p.recordCompletion(ctx, task, time.Since(start), "success")
			return nil
		}

		lastErr = err

		if attempt == p.maxAttempts {
			break
		}

		delay := exponentialBackoff(
			attempt,
			p.config.RetryPolicy.Attempts.MinDelay,
			p.config.RetryPolicy.Attempts.MaxDelay,
		)

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-taskCtx.Done():
			timer.Stop()
			p.recordCompletion(ctx, task, time.Since(start), "cancelled")

			return taskCtx.Err()
		}
	}

	// Логирование финальной ошибки — забота runTask (там известно, есть ли
	// Complete-получатель), а не этого метода.
	p.recordCompletion(ctx, task, time.Since(start), "failed")

	return lastErr
}

// recordCompletion записывает OTel-метрики по завершении задачи.
// status: "success" | "failed" | "cancelled".
//
// ctx — контекст задачи (со span'ом), а не context.Background(): это
// сохраняет связь метрики с трейсом задачи через exemplar'ы у тех
// экспортёров метрик, что их поддерживают. Запись метрики не блокируется
// и не зависит от отмены ctx — принять уже отменённый ctx безопасно.
func (p *pool) recordCompletion(ctx context.Context, task Task, dur time.Duration, status string) {
	attrs := metric.WithAttributes(
		attribute.String("tenant.id", task.TenantID.String()),
		attribute.String("status", status),
	)
	p.taskDuration.Record(ctx, dur.Seconds(), attrs)
	p.tasksTotal.Add(ctx, 1, attrs)
}

// exponentialBackoff вычисляет задержку перед следующей попыткой.
// Применяет полный jitter (случайное значение в [0, cap]) для предотвращения
// thundering herd при одновременном сбое множества задач.
func exponentialBackoff(attempt int, minDelay, maxDelay time.Duration) time.Duration {
	if maxDelay <= 0 {
		return 0
	}
	// Экспоненциальный рост: minDelay * 2^(attempt-1), не превышая maxDelay.
	exp := minDelay
	for range attempt - 1 {
		next := exp * 2
		if next > maxDelay || next < exp { // защита от переполнения int64
			exp = maxDelay
			break
		}

		exp = next
	}

	if exp <= 0 {
		return 0
	}
	// Полный jitter: равномерное случайное значение в [0, exp).
	return rand.N(exp) //nolint:gosec // non-cryptographic jitter for retry backoff, not security-sensitive
}
