package workerpool

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/goleak"
)

// TestMain отключает slog.Default() перед бенчмарками: startManager/Stop
// логируют INFO-события жизненного цикла на каждой итерации setup/teardown,
// и при перенаправлении вывода `go test -bench=... | tee report.txt` эти
// строки перемежаются с результатами бенчмарков построчно, ломая парсер
// benchstat. Сама запись лога происходит вне таймируемого участка
// (b.Loop()/b.RunParallel), так что на ns/op это не влияет — меняется
// только место вывода, не объём работы.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.DiscardHandler))
	goleak.VerifyTestMain(m)
}

// BenchmarkSubmitTask измеряет сквозную стоимость одной задачи: SubmitTask +
// прохождение через диспетчер тенанта и глобальный пул + вызов Complete.
//
// SubmitTask сама по себе — это неблокирующая постановка в очередь (fire-and-
// forget), но состязательный цикл submit-без-ожидания быстро переполнил бы
// tenantQueue: реальный диспетчер+пул (с OTel-спанами и переключениями
// горутин) на порядок медленнее одной операции отправки в канал. taskQueue
// намеренно рассчитан только на одного читателя-диспетчера на тенант (см.
// docs/CONCURRENCY_AUDIT.md, находка №1), поэтому подключать к нему
// дополнительные горутины-потребители в обход этого инварианта нельзя.
// Вместо этого каждый параллельный воркер бенчмарка ждёт Complete перед
// следующей отправкой — глубина очереди никогда не превышает
// GOMAXPROCS-в-полёте, что даёт честную сквозную задержку без риска
// ErrTenantQueueFull.
func BenchmarkSubmitTask(b *testing.B) {
	tenantID := uuid.New()
	provider := &mockTenantProvider{}
	provider.set([]Tenant{&mockTenant{id: tenantID, limit: 32}})

	cfg := newTestConfig()
	cfg.WorkerCount = 32
	cfg.TenantQueueSize = 64
	cfg.TenantRefreshInterval = time.Hour
	m := startManager(b, provider, cfg)

	exec := successExec()

	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		done := make(chan struct{}, 1)
		complete := func(error) { done <- struct{}{} }

		for pb.Next() {
			task := newTask(tenantID, exec, complete)
			if err := m.SubmitTask(tenantID, task); err != nil {
				b.Error(err)

				return
			}

			<-done
		}
	})
}

// BenchmarkExecutorRegistryGet измеряет стоимость разрешения executor'а по
// ключу — путь, проходимый на каждой задаче, использующей Task.ExecutorKey
// вместо прямого Task.Executor (типичный случай при интеграции с River).
func BenchmarkExecutorRegistryGet(b *testing.B) {
	r := NewExecutorRegistry()
	r.MustRegister("job", successExec())

	b.ReportAllocs()

	for b.Loop() {
		if _, err := r.Get("job"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRefreshTenantsSteadyState измеряет стоимость одного цикла
// refreshTenants, когда список тенантов от TenantProvider не изменился —
// именно этот случай выполняется на каждом тике TenantRefreshInterval в
// проде почти всегда (изменения состава тенантов — редкое событие).
// Параметризовано числом тенантов, чтобы видеть масштабирование стоимости
// пересборки wantSet.
func BenchmarkRefreshTenantsSteadyState(b *testing.B) {
	for _, n := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("tenants=%d", n), func(b *testing.B) {
			tenants := make([]Tenant, n)
			for i := range tenants {
				tenants[i] = &mockTenant{id: uuid.New(), limit: 1}
			}

			provider := &mockTenantProvider{}
			provider.set(tenants)

			cfg := newTestConfig()
			cfg.TenantRefreshInterval = time.Hour
			m := startManager(b, provider, cfg)

			b.ReportAllocs()

			for b.Loop() {
				if err := m.refreshTenants(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkExponentialBackoff измеряет стоимость расчёта задержки перед
// повторной попыткой — вызывается на каждой неудачной попытке выполнения
// задачи в executeWithRetry.
func BenchmarkExponentialBackoff(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		exponentialBackoff(5, time.Millisecond, time.Second)
	}
}
