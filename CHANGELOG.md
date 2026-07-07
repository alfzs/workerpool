# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Changelog tracking begins with this file. Patch releases prior to `v2.1.0`
> were not accompanied by changelog entries at the time — see
> [git tags](https://github.com/alfzs/workerpool/tags) for the raw commit
> history of those versions.

## [Unreleased]

## [2.1.5] - 2026-07-07

A documentation and hardening release: a series of skill-driven audits
(safety, security, structs & interfaces, concurrency, context, data
structures, dependency injection, design patterns, modernization,
benchmarking, observability, performance, pkg.go.dev, project layout,
testing) surfaced and fixed several concurrency and correctness issues, and
raised test coverage from 89.0% to 93.8%. No breaking changes.

### Added

- `docs/CODE_STYLE_AUDIT.md` and thirteen further audit documents (safety,
  security, structs & interfaces, concurrency, context, data structures,
  dependency injection, design patterns, modernization, benchmark,
  observability, performance, pkg.go.dev, project layout, testing), README,
  CONTRIBUTING, LICENSE (MIT), llms.txt.
- `ExampleXxx` test functions for `ExecutorRegistry` and `WorkerManager`.
- Baseline benchmarks for `SubmitTask`, `ExecutorRegistry.Get`,
  `refreshTenants`, and `exponentialBackoff`.
- `go.uber.org/goleak` wired into `TestMain` to catch leaked goroutines.
- Nine regression tests closing every branch `go tool cover` reported at 0%.
- `Makefile` and extended `.gitignore` for repo hygiene.

### Fixed

- Held `tenantsMu` across `SubmitTask`'s send to close a race with tenant
  removal.
- Made `setWorkerCount` wait for the previous dispatcher generation to exit
  before starting a new one, closing a generation-handoff race.
- Drained a tenant's task queue on removal to guarantee `Task.Complete` is
  always called.
- Added interface assertions and JSON tags to `HealthStatus`.
- Injected the logger via `WorkerManagerParams` instead of reading
  `slog.Default()` at call time.
- Propagated task span context into `recordCompletion` metrics and threaded
  `context.Context` into internal `slog` calls for trace correlation.
- Applied `Config.TaskTimeout` as the actual task execution deadline (it was
  previously accepted but unused).
- Resolved findings from the golang-safety, error-handling/lint/naming, and
  golang-code-style audits.
- Removed a redundant `else` after a `return` in `pool.executeWithRetry`.
- Reordered `WorkerManager.dispatch` parameters so `context.Context` comes
  first.
- Replaced an index-based counting loop with `range` in
  `exponentialBackoff`.

### Changed

- Default logger construction in `NewWorkerManager` now uses `cmp.Or`.

## [2.1.4] - 2026-06-28

### Docs

- Documented the test suite in `ARCHITECTURE.md`.

## [2.1.3] - 2026-06-28

### Added

- Test suite covering critical concurrency invariants (tenant isolation, semaphore limits, graceful shutdown).

## [2.1.2] - 2026-06-28

### Docs

- Added a full [River](https://github.com/riverqueue/river) integration example to `ARCHITECTURE.md`.

## [2.1.1] - 2026-06-28

### Docs

- Translated `ARCHITECTURE.md` to Russian.

## [2.1.0] - 2026-06-28

### Added

- `ExecutorRegistry`, replacing the previous task registry: maps string keys to `TaskExecutor` implementations so job stores can persist a key instead of a concrete executor.
- `WorkerManager.Health()` / `HealthStatus` / `TenantHealth` — a point-in-time snapshot of queue depth, worker counts, and per-tenant state for liveness/readiness probes.
- `ARCHITECTURE.md` describing the component map and River integration.

### Changed

- Internal architecture of `WorkerManager` and the worker pool reworked for production readiness (tenant dispatch, semaphore handling, retry/backoff paths).

## [2.0.2] - 2026-03-01

### Changed

- **Breaking:** module path updated to `github.com/alfzs/workerpool/v2`.
- Executor handling refactored (`tenant_executor.go` replaced by `executor.go`).

## [2.0.0] - 2026-02-28

### Added

- Initial v2 release.

[Unreleased]: https://github.com/alfzs/workerpool/compare/v2.1.5...HEAD
[2.1.5]: https://github.com/alfzs/workerpool/releases/tag/v2.1.5
[2.1.4]: https://github.com/alfzs/workerpool/releases/tag/v2.1.4
[2.1.3]: https://github.com/alfzs/workerpool/releases/tag/v2.1.3
[2.1.2]: https://github.com/alfzs/workerpool/releases/tag/v2.1.2
[2.1.1]: https://github.com/alfzs/workerpool/releases/tag/v2.1.1
[2.1.0]: https://github.com/alfzs/workerpool/releases/tag/v2.1.0
[2.0.2]: https://github.com/alfzs/workerpool/releases/tag/v2.0.2
[2.0.0]: https://github.com/alfzs/workerpool/releases/tag/v2.0.0
