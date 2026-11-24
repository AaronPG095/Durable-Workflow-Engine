package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job status values matching the PostgreSQL CHECK constraint.
const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusDead      = "dead"
	StatusCancelled = "cancelled"
)

// Job represents a single durable task in the PostgreSQL queue.
type Job struct {
	ID             uuid.UUID       `json:"id"`
	Queue          string          `json:"queue"`
	JobType        string          `json:"job_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	ConcurrencyKey *string         `json:"concurrency_key,omitempty"`
	Status         string          `json:"status"`
	Priority       int16           `json:"priority"`
	RunAt          time.Time       `json:"run_at"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	LeaseOwner     *string         `json:"lease_owner,omitempty"`
	LeaseToken     *uuid.UUID      `json:"lease_token,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	HeartbeatAt    *time.Time      `json:"heartbeat_at,omitempty"`
	WorkflowRunID  *uuid.UUID      `json:"workflow_run_id,omitempty"`
	LastError      json.RawMessage `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

// DeadLetterJob represents an immutable DLQ record and optional replay linkage.
type DeadLetterJob struct {
	ID            uuid.UUID       `json:"id"`
	OriginalJobID uuid.UUID       `json:"original_job_id"`
	Queue         string          `json:"queue"`
	JobType       string          `json:"job_type"`
	Payload       json.RawMessage `json:"payload"`
	Attempts      int             `json:"attempts"`
	FailureReason string          `json:"failure_reason"`
	ErrorHistory  json.RawMessage `json:"error_history"`
	FailedAt      time.Time       `json:"failed_at"`
	ReplayedAt    *time.Time      `json:"replayed_at,omitempty"`
	ReplayedJobID *uuid.UUID      `json:"replayed_job_id,omitempty"`
}

// EnqueueParams configures a new job submission.
type EnqueueParams struct {
	Queue          string
	JobType        string
	Payload        json.RawMessage
	IdempotencyKey string
	ConcurrencyKey string
	Priority       int16
	RunAt          time.Time
	MaxAttempts    int
	WorkflowRunID  *uuid.UUID
}

// ClaimParams configures an atomic batch claim.
type ClaimParams struct {
	WorkerID              string
	Queue                 string
	Limit                 int
	LeaseTTL              time.Duration
	QueueConcurrencyLimit int
	KeyConcurrencyLimit   int
}

// FailOutcome reports whether a failed job was rescheduled or dead-lettered.
type FailOutcome struct {
	FencedOut   bool
	DeadLettered bool
	NextRunAt   *time.Time
}

// ReapResult summarizes a lease-expiry reaper sweep.
type ReapResult struct {
	Requeued     int
	DeadLettered int
}

// QueueStats summarizes counts per status for observability.
type QueueStats struct {
	Queue     string `json:"queue"`
	Queued    int64  `json:"queued"`
	Running   int64  `json:"running"`
	Completed int64  `json:"completed"`
	Dead      int64  `json:"dead"`
	Cancelled int64  `json:"cancelled"`
}

// ErrAlreadyReplayed is returned when attempting to replay a DLQ entry twice.
var ErrAlreadyReplayed = errors.New("dead-letter job already replayed")

// Store implements Layer 1 durable queue operations on PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pgxpool.Pool for queue operations.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const jobColumns = `
	id, queue, job_type, payload, idempotency_key, concurrency_key,
	status, priority, run_at, attempt, max_attempts,
	lease_owner, lease_token, lease_expires_at, heartbeat_at,
	workflow_run_id, last_error, created_at, updated_at, completed_at
`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(
		&j.ID, &j.Queue, &j.JobType, &j.Payload, &j.IdempotencyKey, &j.ConcurrencyKey,
		&j.Status, &j.Priority, &j.RunAt, &j.Attempt, &j.MaxAttempts,
		&j.LeaseOwner, &j.LeaseToken, &j.LeaseExpiresAt, &j.HeartbeatAt,
		&j.WorkflowRunID, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.CompletedAt,
	)
	return j, err
}

// Enqueue inserts a new job or returns the existing one if (queue, idempotency_key) collides.
func (s *Store) Enqueue(ctx context.Context, p EnqueueParams) (Job, bool, error) {
	if p.JobType == "" {
		return Job{}, false, fmt.Errorf("job_type is required")
	}
	q := p.Queue
	if q == "" {
		q = "default"
	}
	payload := p.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	runAt := p.RunAt
	if runAt.IsZero() {
		runAt = time.Now().UTC()
	}

	var idempKey *string
	if p.IdempotencyKey != "" {
		k := p.IdempotencyKey
		idempKey = &k
	}
	var concKey *string
	if p.ConcurrencyKey != "" {
		k := p.ConcurrencyKey
		concKey = &k
	}

	if idempKey == nil {
		row := s.pool.QueryRow(ctx, `
			INSERT INTO jobs (
				queue, job_type, payload, idempotency_key, concurrency_key,
				priority, run_at, max_attempts, workflow_run_id
			) VALUES ($1, $2, $3, NULL, $4, $5, $6, $7, $8)
			RETURNING `+jobColumns,
			q, p.JobType, payload, concKey, p.Priority, runAt, maxAttempts, p.WorkflowRunID,
		)
		j, err := scanJob(row)
		return j, false, err
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (
			queue, job_type, payload, idempotency_key, concurrency_key,
			priority, run_at, max_attempts, workflow_run_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING `+jobColumns,
		q, p.JobType, payload, idempKey, concKey, p.Priority, runAt, maxAttempts, p.WorkflowRunID,
	)
	j, err := scanJob(row)
	if err == nil {
		return j, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, err
	}

	// Conflict occurred: fetch the canonical existing job.
	existing := s.pool.QueryRow(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE queue = $1 AND idempotency_key = $2
	`, q, *idempKey)
	j, err = scanJob(existing)
	if err != nil {
		return Job{}, false, fmt.Errorf("fetch deduplicated job: %w", err)
	}
	return j, true, nil
}

// ClaimBatch atomically claims up to Limit runnable jobs using FOR UPDATE SKIP LOCKED
// and optional per-queue / per-key concurrency limits guarded by non-blocking advisory locks.
func (s *Store) ClaimBatch(ctx context.Context, p ClaimParams) ([]Job, error) {
	if p.WorkerID == "" {
		return nil, fmt.Errorf("worker_id is required")
	}
	q := p.Queue
	if q == "" {
		q = "default"
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 1
	}
	leaseTTL := p.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 15 * time.Second
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var queueRunning int
	if p.QueueConcurrencyLimit > 0 {
		var locked bool
		if err := tx.QueryRow(ctx,
			`SELECT pg_try_advisory_xact_lock(hashtext($1))`,
			"queue:"+q,
		).Scan(&locked); err != nil {
			return nil, err
		}
		if !locked {
			return nil, nil
		}
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM jobs WHERE queue = $1 AND status = 'running'`,
			q,
		).Scan(&queueRunning); err != nil {
			return nil, err
		}
		available := p.QueueConcurrencyLimit - queueRunning
		if available <= 0 {
			return nil, nil
		}
		if available < limit {
			limit = available
		}
	}

	// Scan a bounded window when key concurrency limits are enabled so blocked keys
	// don't starve other eligible keys.
	fetchLimit := limit
	if p.KeyConcurrencyLimit > 0 && fetchLimit < limit*4 {
		fetchLimit = limit * 4
	}

	rows, err := tx.Query(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE queue = $1 AND status = 'queued' AND run_at <= now()
		ORDER BY priority DESC, run_at ASC, created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $2
	`, q, fetchLimit)
	if err != nil {
		return nil, err
	}

	var candidates []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(
			&j.ID, &j.Queue, &j.JobType, &j.Payload, &j.IdempotencyKey, &j.ConcurrencyKey,
			&j.Status, &j.Priority, &j.RunAt, &j.Attempt, &j.MaxAttempts,
			&j.LeaseOwner, &j.LeaseToken, &j.LeaseExpiresAt, &j.HeartbeatAt,
			&j.WorkflowRunID, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.CompletedAt,
		); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, j)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	if len(candidates) == 0 {
		return nil, nil
	}

	keyCounts := make(map[string]int)
	var claimed []Job

	for _, c := range candidates {
		if len(claimed) >= limit {
			break
		}
		if p.KeyConcurrencyLimit > 0 && c.ConcurrencyKey != nil && *c.ConcurrencyKey != "" {
			ck := *c.ConcurrencyKey
			if _, seen := keyCounts[ck]; !seen {
				var locked bool
				if err := tx.QueryRow(ctx,
					`SELECT pg_try_advisory_xact_lock(hashtext($1))`,
					"key:"+q+":"+ck,
				).Scan(&locked); err != nil {
					return nil, err
				}
				if !locked {
					continue
				}
				var runningForKey int
				if err := tx.QueryRow(ctx,
					`SELECT count(*) FROM jobs WHERE queue = $1 AND concurrency_key = $2 AND status = 'running'`,
					q, ck,
				).Scan(&runningForKey); err != nil {
					return nil, err
				}
				keyCounts[ck] = runningForKey
			}
			if keyCounts[ck] >= p.KeyConcurrencyLimit {
				continue
			}
			keyCounts[ck]++
		}

		updatedRow := tx.QueryRow(ctx, `
			UPDATE jobs
			SET status = 'running',
				attempt = attempt + 1,
				lease_owner = $2,
				lease_token = gen_random_uuid(),
				lease_expires_at = now() + ($3 * interval '1 millisecond'),
				heartbeat_at = now(),
				updated_at = now()
			WHERE id = $1
			RETURNING `+jobColumns,
			c.ID, p.WorkerID, leaseTTL.Milliseconds(),
		)
		j, err := scanJob(updatedRow)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, j)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

// Heartbeat extends lease_expires_at only if the caller still holds the active lease_token.
// Returns false if the lease was lost (self-fencing signal).
func (s *Store) Heartbeat(ctx context.Context, jobID, leaseToken uuid.UUID, leaseTTL time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET lease_expires_at = now() + ($3 * interval '1 millisecond'),
			heartbeat_at = now(),
			updated_at = now()
		WHERE id = $1 AND status = 'running' AND lease_token = $2
	`, jobID, leaseToken, leaseTTL.Milliseconds())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Complete marks a job completed iff it is still running with the matching leaseToken.
func (s *Store) Complete(ctx context.Context, jobID, leaseToken uuid.UUID) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'completed',
			completed_at = now(),
			updated_at = now(),
			lease_owner = NULL,
			lease_token = NULL,
			lease_expires_at = NULL
		WHERE id = $1 AND status = 'running' AND lease_token = $2
	`, jobID, leaseToken)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// CompleteWithTx marks a job completed inside a caller-managed transaction so domain
// side effects and job completion commit atomically.
func (s *Store) CompleteWithTx(ctx context.Context, tx pgx.Tx, jobID, leaseToken uuid.UUID) (bool, error) {
	tag, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'completed',
			completed_at = now(),
			updated_at = now(),
			lease_owner = NULL,
			lease_token = NULL,
			lease_expires_at = NULL
		WHERE id = $1 AND status = 'running' AND lease_token = $2
	`, jobID, leaseToken)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Fail records a job failure fenced on leaseToken. Retryable failures below max_attempts
// are rescheduled with full-jitter exponential backoff; otherwise the job moves to dead_letter_jobs.
func (s *Store) Fail(ctx context.Context, jobID, leaseToken uuid.UUID, errMsg string, retryable bool, backoffCfg BackoffConfig) (FailOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return FailOutcome{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE id = $1 AND status = 'running' AND lease_token = $2
		FOR UPDATE
	`, jobID, leaseToken)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return FailOutcome{FencedOut: true}, nil
	}
	if err != nil {
		return FailOutcome{}, err
	}

	outcome, err := s.applyFailureTx(ctx, tx, j, errMsg, retryable, backoffCfg)
	if err != nil {
		return FailOutcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return FailOutcome{}, err
	}
	return outcome, nil
}

func (s *Store) applyFailureTx(ctx context.Context, tx pgx.Tx, j Job, errMsg string, retryable bool, backoffCfg BackoffConfig) (FailOutcome, error) {
	errJSON, _ := json.Marshal(map[string]any{
		"message":   errMsg,
		"attempt":   j.Attempt,
		"failed_at": time.Now().UTC(),
	})

	if retryable && j.Attempt < j.MaxAttempts {
		delay := FullJitter(j.Attempt, backoffCfg, nil)
		nextRun := time.Now().UTC().Add(delay)
		if _, err := tx.Exec(ctx, `
			UPDATE jobs
			SET status = 'queued',
				run_at = $2,
				last_error = $3,
				lease_owner = NULL,
				lease_token = NULL,
				lease_expires_at = NULL,
				updated_at = now()
			WHERE id = $1
		`, j.ID, nextRun, errJSON); err != nil {
			return FailOutcome{}, err
		}
		return FailOutcome{NextRunAt: &nextRun}, nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'dead',
			last_error = $2,
			lease_owner = NULL,
			lease_token = NULL,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE id = $1
	`, j.ID, errJSON); err != nil {
		return FailOutcome{}, err
	}

	historyJSON, _ := json.Marshal([]json.RawMessage{errJSON})
	if _, err := tx.Exec(ctx, `
		INSERT INTO dead_letter_jobs (
			original_job_id, queue, job_type, payload, idempotency_key,
			attempts, failure_reason, error_history
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, j.ID, j.Queue, j.JobType, j.Payload, j.IdempotencyKey, j.Attempt, errMsg, historyJSON); err != nil {
		return FailOutcome{}, err
	}
	return FailOutcome{DeadLettered: true}, nil
}

// ReapExpired recovers jobs whose lease_expires_at has passed using FOR UPDATE SKIP LOCKED
// so multiple reapers never contend.
func (s *Store) ReapExpired(ctx context.Context, batchSize int, backoffCfg BackoffConfig) (ReapResult, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReapResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE status = 'running' AND lease_expires_at < now()
		ORDER BY lease_expires_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	`, batchSize)
	if err != nil {
		return ReapResult{}, err
	}

	var expired []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(
			&j.ID, &j.Queue, &j.JobType, &j.Payload, &j.IdempotencyKey, &j.ConcurrencyKey,
			&j.Status, &j.Priority, &j.RunAt, &j.Attempt, &j.MaxAttempts,
			&j.LeaseOwner, &j.LeaseToken, &j.LeaseExpiresAt, &j.HeartbeatAt,
			&j.WorkflowRunID, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.CompletedAt,
		); err != nil {
			rows.Close()
			return ReapResult{}, err
		}
		expired = append(expired, j)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ReapResult{}, err
	}
	rows.Close()

	var res ReapResult
	for _, j := range expired {
		outcome, err := s.applyFailureTx(ctx, tx, j, "LEASE_EXPIRED", true, backoffCfg)
		if err != nil {
			return ReapResult{}, err
		}
		if outcome.DeadLettered {
			res.DeadLettered++
		} else {
			res.Requeued++
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ReapResult{}, err
	}
	return res, nil
}

// ListDLQ returns recent dead-letter jobs, optionally filtered by queue.
func (s *Store) ListDLQ(ctx context.Context, queue string, limit int) ([]DeadLetterJob, error) {
	if limit <= 0 {
		limit = 50
	}
	var rows pgx.Rows
	var err error
	if queue != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, original_job_id, queue, job_type, payload, attempts,
			       failure_reason, error_history, failed_at, replayed_at, replayed_job_id
			FROM dead_letter_jobs
			WHERE queue = $1
			ORDER BY failed_at DESC
			LIMIT $2
		`, queue, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, original_job_id, queue, job_type, payload, attempts,
			       failure_reason, error_history, failed_at, replayed_at, replayed_job_id
			FROM dead_letter_jobs
			ORDER BY failed_at DESC
			LIMIT $1
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DeadLetterJob
	for rows.Next() {
		var d DeadLetterJob
		if err := rows.Scan(
			&d.ID, &d.OriginalJobID, &d.Queue, &d.JobType, &d.Payload, &d.Attempts,
			&d.FailureReason, &d.ErrorHistory, &d.FailedAt, &d.ReplayedAt, &d.ReplayedJobID,
		); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ReplayDLQ atomically replays a dead-letter job once by creating a fresh queued job.
func (s *Store) ReplayDLQ(ctx context.Context, dlqID uuid.UUID) (Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var origID uuid.UUID
	var q, jobType string
	var payload json.RawMessage
	var replayedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT original_job_id, queue, job_type, payload, replayed_at
		FROM dead_letter_jobs
		WHERE id = $1
		FOR UPDATE
	`, dlqID).Scan(&origID, &q, &jobType, &payload, &replayedAt)
	if err != nil {
		return Job{}, err
	}
	if replayedAt != nil {
		return Job{}, ErrAlreadyReplayed
	}

	var concKey *string
	var priority int16
	var maxAttempts int
	if err := tx.QueryRow(ctx, `
		SELECT concurrency_key, priority, max_attempts FROM jobs WHERE id = $1
	`, origID).Scan(&concKey, &priority, &maxAttempts); err != nil {
		return Job{}, err
	}

	newJobRow := tx.QueryRow(ctx, `
		INSERT INTO jobs (queue, job_type, payload, concurrency_key, priority, max_attempts)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING `+jobColumns,
		q, jobType, payload, concKey, priority, maxAttempts,
	)
	newJob, err := scanJob(newJobRow)
	if err != nil {
		return Job{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE dead_letter_jobs
		SET replayed_at = now(), replayed_job_id = $2
		WHERE id = $1 AND replayed_at IS NULL
	`, dlqID, newJob.ID)
	if err != nil {
		return Job{}, err
	}
	if tag.RowsAffected() != 1 {
		return Job{}, ErrAlreadyReplayed
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return newJob, nil
}

// Stats returns aggregated job counts grouped by queue.
func (s *Store) Stats(ctx context.Context) ([]QueueStats, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT queue,
			count(*) FILTER (WHERE status = 'queued')    AS queued,
			count(*) FILTER (WHERE status = 'running')   AS running,
			count(*) FILTER (WHERE status = 'completed') AS completed,
			count(*) FILTER (WHERE status = 'dead')      AS dead,
			count(*) FILTER (WHERE status = 'cancelled') AS cancelled
		FROM jobs
		GROUP BY queue
		ORDER BY queue ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []QueueStats
	for rows.Next() {
		var st QueueStats
		if err := rows.Scan(&st.Queue, &st.Queued, &st.Running, &st.Completed, &st.Dead, &st.Cancelled); err != nil {
			return nil, err
		}
		stats = append(stats, st)
	}
	return stats, rows.Err()
}
