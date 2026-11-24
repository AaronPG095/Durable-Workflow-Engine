---
title: Blueprint: Durable Workflow Engine Architecture
status: active
plannerModel: gemini-4-neon-medium
created: 2026-11-24
---
# Architecture Blueprint: Durable Workflow Engine Architecture

## Goal & Non-Goals

Build a two-layer distributed systems monorepo in **Go** (`pgx/v5` + PostgreSQL 17):
- **`engine/`**: General-purpose durable task queue & checkpointed workflow engine with **zero AI-specific coupling**.
- **`examples/software-factory/`**: Flagship real-world workload built purely on top of the `engine/` SDK.

**Non-goals:** Multi-database abstraction layers (PostgreSQL features like `FOR UPDATE SKIP LOCKED`, partial indexes, and `pg_try_advisory_xact_lock` are first-class); embedding LLM/git-worktree logic inside `engine/`.

## Architecture & Boundaries

```
examples/software-factory/   (Domain workload: git worktrees, CI gates, review/fix loop, human approval)
        │  uses public SDK only
        ▼
engine/pkg/dwe/              (Public Go SDK: Client, Worker, Workflow Runner)
   ├── Layer 2: Workflow     (Checkpointed steps, fan-out/fan-in DAGs, durable timers/signals, Saga)
   └── Layer 1: Queue        (SKIP LOCKED claiming, fenced leases, heartbeats, backoff, concurrency, DLQ)
        │
        ▼
PostgreSQL                   (jobs, dead_letter_jobs, workflow_runs, workflow_steps, workflow_signals)
```

## Contracts & Invariants

1. **Five Core Tables:** `jobs`, `dead_letter_jobs`, `workflow_runs`, `workflow_steps`, `workflow_signals` with partial indexes on `status = 'queued'` and `status = 'running'`.
2. **Fencing Tokens (`lease_token`):** Every claim rotates `lease_token`. Every heartbeat, completion, failure, and checkpoint mutation predicates on `WHERE id = $1 AND status = 'running' AND lease_token = $2`. Zero updated rows triggers immediate context cancellation (self-fencing).
3. **Atomic Side Effects:** Handlers may commit domain writes and `CompleteWithTx` in the same PostgreSQL transaction for exactly-once effects under `SIGKILL`.
4. **Zero-Memory Suspend:** Workflows waiting on timers or external signals release all worker memory and resume via deduplicated `workflow.resume` jobs.
5. **Out-of-Order Signal Safety:** `sendSignal` upserts `(workflow_run_id, signal_name, signal_key)` so signals arriving before `waitForSignal` are never lost.

## PR Slice Breakdown

1. **Phase 1 — Core Durable Queue + Chaos Test:** PostgreSQL schema/migrations, `Enqueue` (idempotency), `ClaimBatch` (`SKIP LOCKED` + advisory-lock concurrency limits), fenced `Heartbeat`/`Complete`/`CompleteWithTx`/`Fail`, full-jitter exponential backoff, `ReapExpired`, DLQ + atomic replay, `cmd/dwe` CLI, `cmd/dwe-worker`, and multi-process `SIGKILL` chaos test.
2. **Phase 2 — Checkpointed Workflow Engine + Observability:** `step()` memoization, fan-out/fan-in DAGs, durable timers, `waitForSignal`, reverse-order Saga compensation, and lightweight Web UI + CLI inspection.
3. **Phase 3 — `examples/software-factory/` End-to-End Demo:** Isolated `git worktree` provisioning, parallel builders, deterministic CI gate first, bounded `reviewer <-> fixer` loop, durable `human_approval` signal, PR packaging, and Saga worktree cleanup.
4. **Phase 4 — Benchmarks, Architecture Docs & Portfolio Polish:** Load benchmark (1 vs 4 workers, jobs/sec & p95 pickup latency), `ARCHITECTURE.md`, `/docs/decisions/` ADRs, and 90-second `README.md`.

## Living Updates & ADRs

- Stack confirmed: Go (`pgx/v5`), CLI first + lightweight Web UI in Phase 2/4, `tracker: "local"` (`DWE`).
