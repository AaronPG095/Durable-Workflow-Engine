package dwe

import (
	"context"

	"github.com/aaron/durable-workflow-engine/engine/internal/db"
	"github.com/aaron/durable-workflow-engine/engine/internal/queue"
	"github.com/aaron/durable-workflow-engine/engine/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Re-exported public types so external workloads (e.g. examples/software-factory)
// depend only on pkg/dwe without importing internal packages.
type (
	Job            = queue.Job
	DeadLetterJob  = queue.DeadLetterJob
	EnqueueParams  = queue.EnqueueParams
	QueueStats     = queue.QueueStats
	BackoffConfig  = queue.BackoffConfig
	WorkerConfig   = worker.Config
	HandlerFunc    = worker.HandlerFunc
)

// ErrAlreadyReplayed is returned when replaying a dead-letter job more than once.
var ErrAlreadyReplayed = queue.ErrAlreadyReplayed

// NonRetryable marks an error as terminal so the job moves directly to the DLQ.
func NonRetryable(err error) error { return worker.NonRetryable(err) }

// Client provides durable queue and migration operations backed by PostgreSQL.
type Client struct {
	pool  *pgxpool.Pool
	store *queue.Store
}

// Open connects to PostgreSQL using pgxpool.
func Open(ctx context.Context, databaseURL string) (*Client, error) {
	pool, err := db.OpenPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	return &Client{
		pool:  pool,
		store: queue.NewStore(pool),
	}, nil
}

// Close closes the underlying connection pool.
func (c *Client) Close() { c.pool.Close() }

// Pool exposes the underlying pgxpool.Pool for callers needing transactional side effects.
func (c *Client) Pool() *pgxpool.Pool { return c.pool }

// Migrate applies all embedded SQL schema migrations idempotently.
func (c *Client) Migrate(ctx context.Context) error {
	return db.Migrate(ctx, c.pool)
}

// Enqueue submits a durable job, deduplicating by (queue, idempotency_key).
func (c *Client) Enqueue(ctx context.Context, params EnqueueParams) (Job, bool, error) {
	return c.store.Enqueue(ctx, params)
}

// CompleteWithTx marks a job completed inside a caller-managed transaction fenced on leaseToken.
func (c *Client) CompleteWithTx(ctx context.Context, tx pgx.Tx, jobID, leaseToken uuid.UUID) (bool, error) {
	return c.store.CompleteWithTx(ctx, tx, jobID, leaseToken)
}

// ListDLQ lists recent dead-letter jobs.
func (c *Client) ListDLQ(ctx context.Context, queueName string, limit int) ([]DeadLetterJob, error) {
	return c.store.ListDLQ(ctx, queueName, limit)
}

// ReplayDLQ atomically replays a dead-letter job once.
func (c *Client) ReplayDLQ(ctx context.Context, dlqID uuid.UUID) (Job, error) {
	return c.store.ReplayDLQ(ctx, dlqID)
}

// Stats returns per-queue job counts.
func (c *Client) Stats(ctx context.Context) ([]QueueStats, error) {
	return c.store.Stats(ctx)
}

// NewWorker constructs a Worker connected to this Client's PostgreSQL store.
func (c *Client) NewWorker(cfg WorkerConfig) *worker.Worker {
	return worker.New(c.store, cfg)
}
