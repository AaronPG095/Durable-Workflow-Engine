---
title: Plan - Feature: Phase 1 Core Durable Queue and Chaos Test
ticket: local://DWE:DWE-Plan-Phase-1-Core-Durable-Queue-and-Chaos-Test-2026-11
status: planned
priority: 
state: 
due: 
appetite: L
version: 
impact: 
dependsOn: 
blueprint: docs/architecture/DWE-Blueprint-Durable-Workflow-Engine-Architecture.md
devTicketKey: DWE:DWE-Plan-Phase-1-Core-Durable-Queue-and-Chaos-Test-2026-11
agent: Gemini
---
# Phase 1 Core Durable Queue and Chaos Test

## Summary

Implement Layer 1 of `engine/` in Go (`pgx/v5` + PostgreSQL 17): the 5-table PostgreSQL schema and embedded migration runner, `FOR UPDATE SKIP LOCKED` multi-worker claiming, fenced leases and heartbeats, exponential backoff with full jitter, per-queue/per-key concurrency limits, lease-expiry reaper, DLQ with atomic replay, CLI (`cmd/dwe` & `cmd/dwe-worker`), `docker-compose.yml`, and a multi-process `SIGKILL` chaos test.

## Context

See architecture blueprint: `docs/architecture/DWE-Blueprint-Durable-Workflow-Engine-Architecture.md`

## Open questions

None

## Acceptance criteria

- [ ] AC1: Embedded SQL migration (`001_initial_schema.sql`) creates `jobs`, `dead_letter_jobs`, `workflow_runs`, `workflow_steps`, and `workflow_signals` with partial/unique indexes and zero AI-specific coupling.
- [ ] AC2: `Enqueue` deduplicates by `(queue, idempotency_key)` and `ClaimBatch` safely claims jobs across concurrent workers using `FOR UPDATE SKIP LOCKED` and advisory-lock concurrency limits.
- [ ] AC3: Fenced `Heartbeat`, `Complete`, `CompleteWithTx`, and `Fail` reject stale `lease_token`s; worker cancels in-flight job context on lost lease.
- [ ] AC4: Retryable failures reschedule via full-jitter exponential backoff; exhausted or non-retryable failures move atomically to `dead_letter_jobs` and support one-time `ReplayDLQ`.
- [ ] AC5: `ReapExpired` recovers expired `running` leases across workers; `cmd/dwe` (`migrate`, `enqueue`, `status`, `dlq list`, `dlq replay`) and `cmd/dwe-worker` build cleanly.
- [ ] AC6: Unit tests (`go test ./internal/...`), integration tests (`queue_integration_test.go`), and multi-process `SIGKILL` chaos test (`chaos_test.go`) pass with 100% job completion and 0 duplicate side effects.

## Steps

- [ ] Scaffold `engine/go.mod`, `engine/migrations/001_initial_schema.sql`, `engine/internal/db/`, `engine/Dockerfile`, and `docker-compose.yml` (AC1)
- [ ] Implement `engine/internal/queue/backoff.go` and `backoff_test.go` (AC4)
- [ ] Implement `engine/internal/queue/queue.go` with `Enqueue`, `ClaimBatch`, fenced `Heartbeat`/`Complete`/`CompleteWithTx`/`Fail`, `ReapExpired`, `ListDLQ`, `ReplayDLQ`, and `Stats` (AC2, AC3, AC4, AC5)
- [ ] Implement `engine/internal/worker/worker.go`, `engine/pkg/dwe/dwe.go`, `engine/cmd/dwe-worker/main.go`, and `engine/cmd/dwe/main.go` (AC3, AC5)
- [ ] Add `engine/tests/queue_integration_test.go` and `engine/tests/chaos_test.go`, and run `go vet` + unit/integration/chaos tests (AC6)

## Risks

- **Split-brain zombie writes:** A worker paused by GC or network stall wakes up after its lease expired and writes duplicate side effects. Mitigated by rotating `lease_token` on every claim, fencing all state transitions on `lease_token`, cancelling the job's `context.Context` when `Heartbeat` returns `false`, and providing `CompleteWithTx` for atomic domain + job completion.
- **Advisory lock contention or deadlocks on concurrency limits:** Mitigated by using non-blocking `pg_try_advisory_xact_lock` and skipping keys that cannot acquire the lock immediately.

## Out of scope

- Layer 2 workflow execution (`step()` memoization, fan-out/fan-in DAGs, `waitForSignal`, Saga compensation) — covered in Phase 2.
- Web UI dashboard and `examples/software-factory/` — covered in Phase 2 and Phase 3.

## Notes

- Aaron confirmed stack choices:
  > A1: (Recommended) Go (pgx/v5, native goroutines & context fencing)
  > A2: (Recommended) CLI first (Phase 1-2) + lightweight Web UI (Phase 2/4) for instant docker compose up demo
  > A3: (Recommended) Local tracker (tracker: "local", zero external tokens, public-repo ready)

## Progress log

<!-- One line per session/review, added with `ticket.mjs log` or `ticket.mjs review` -->
