-- 001_initial_schema.sql
-- Layer 1 (Durable Task Queue) & Layer 2 (Durable Workflow Engine) schema.
-- Zero AI-specific coupling.

-- 1. Layer 1: Durable Task Queue
CREATE TABLE IF NOT EXISTS jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue            TEXT NOT NULL DEFAULT 'default',
    job_type         TEXT NOT NULL,
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key  TEXT,
    concurrency_key  TEXT,
    status           TEXT NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued', 'running', 'completed', 'dead', 'cancelled')),
    priority         SMALLINT NOT NULL DEFAULT 0,
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt          INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 5,
    lease_owner      TEXT,
    lease_token      UUID,
    lease_expires_at TIMESTAMPTZ,
    heartbeat_at     TIMESTAMPTZ,
    workflow_run_id  UUID,
    last_error       JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
    ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_claimable
    ON jobs (queue, priority DESC, run_at ASC, created_at ASC) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_jobs_expired_leases
    ON jobs (lease_expires_at ASC) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_jobs_running_concurrency
    ON jobs (queue, concurrency_key) WHERE status = 'running';

-- 2. Layer 1: Dead-Letter Queue
CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_job_id  UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    queue            TEXT NOT NULL,
    job_type         TEXT NOT NULL,
    payload          JSONB NOT NULL,
    idempotency_key  TEXT,
    attempts         INT NOT NULL,
    failure_reason   TEXT NOT NULL,
    error_history    JSONB NOT NULL DEFAULT '[]'::jsonb,
    failed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at      TIMESTAMPTZ,
    replayed_job_id  UUID REFERENCES jobs(id)
);

CREATE INDEX IF NOT EXISTS idx_dlq_queue_failed_at
    ON dead_letter_jobs (queue, failed_at DESC);

-- 3. Layer 2: Workflow Runs
CREATE TABLE IF NOT EXISTS workflow_runs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name    TEXT NOT NULL,
    workflow_version INT NOT NULL DEFAULT 1,
    idempotency_key  TEXT,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN (
                         'pending', 'running', 'waiting',
                         'compensating', 'completed', 'failed', 'cancelled'
                     )),
    input            JSONB NOT NULL DEFAULT '{}'::jsonb,
    output           JSONB,
    error            JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_runs_idempotency
    ON workflow_runs (workflow_name, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_status
    ON workflow_runs (status, updated_at DESC);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_jobs_workflow_run'
    ) THEN
        ALTER TABLE jobs
            ADD CONSTRAINT fk_jobs_workflow_run
            FOREIGN KEY (workflow_run_id) REFERENCES workflow_runs(id) ON DELETE CASCADE;
    END IF;
END $$;

-- 4. Layer 2: Checkpointed Step Log
CREATE TABLE IF NOT EXISTS workflow_steps (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id      UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    step_key             TEXT NOT NULL,
    step_type            TEXT NOT NULL
                         CHECK (step_type IN ('task', 'timer', 'signal', 'fanout', 'compensation')),
    status               TEXT NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending', 'running', 'completed', 'failed', 'compensated')),
    depends_on           TEXT[] NOT NULL DEFAULT '{}',
    attempt              INT NOT NULL DEFAULT 0,
    input                JSONB,
    output               JSONB,
    error                JSONB,
    compensation_handler TEXT,
    compensation_input   JSONB,
    job_id               UUID REFERENCES jobs(id),
    started_at           TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workflow_run_id, step_key)
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_run_status
    ON workflow_steps (workflow_run_id, status);

-- 5. Layer 2: Durable External Signals
CREATE TABLE IF NOT EXISTS workflow_signals (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_run_id  UUID NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    signal_name      TEXT NOT NULL,
    signal_key       TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT 'waiting'
                     CHECK (status IN ('waiting', 'received', 'expired')),
    payload          JSONB,
    expires_at       TIMESTAMPTZ,
    received_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workflow_run_id, signal_name, signal_key)
);
