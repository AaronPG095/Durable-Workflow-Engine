package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/aaron/durable-workflow-engine/engine/internal/queue"
	"github.com/google/uuid"
)

// NonRetryableError marks an error as terminal so the job goes directly to the DLQ.
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string { return e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NonRetryable wraps err so the worker dead-letters the job immediately without retries.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &NonRetryableError{Err: err}
}

// HandlerFunc processes a single claimed job.
// If the handler commits completion itself (e.g. via Store.CompleteWithTx), it can return nil;
// Worker.Complete safely treats already-completed jobs as a no-op.
type HandlerFunc func(ctx context.Context, job queue.Job) error

// Config configures a Worker instance.
type Config struct {
	WorkerID              string
	Queue                 string
	Concurrency           int
	PollInterval          time.Duration
	LeaseTTL              time.Duration
	ReaperInterval        time.Duration
	ShutdownGracePeriod   time.Duration
	Backoff               queue.BackoffConfig
	QueueConcurrencyLimit int
	KeyConcurrencyLimit   int
	Logger                *slog.Logger
}

// Worker polls PostgreSQL for jobs, maintains fenced heartbeats, and runs the lease reaper.
type Worker struct {
	store    *queue.Store
	cfg      Config
	handlers map[string]HandlerFunc
	mu       sync.RWMutex
	logger   *slog.Logger
}

// New creates a Worker with normalized defaults.
func New(store *queue.Store, cfg Config) *Worker {
	if cfg.WorkerID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "worker"
		}
		cfg.WorkerID = fmt.Sprintf("%s-%s", host, uuid.NewString()[:8])
	}
	if cfg.Queue == "" {
		cfg.Queue = "default"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 15 * time.Second
	}
	if cfg.ReaperInterval <= 0 {
		cfg.ReaperInterval = 2 * time.Second
	}
	if cfg.ShutdownGracePeriod <= 0 {
		cfg.ShutdownGracePeriod = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Worker{
		store:    store,
		cfg:      cfg,
		handlers: make(map[string]HandlerFunc),
		logger:   cfg.Logger,
	}
}

// Register binds a jobType to its HandlerFunc.
func (w *Worker) Register(jobType string, h HandlerFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[jobType] = h
}

func (w *Worker) handlerFor(jobType string) (HandlerFunc, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	h, ok := w.handlers[jobType]
	return h, ok
}

// Run starts the claim loop and reaper loop, blocking until ctx is cancelled
// and in-flight jobs finish (bounded by ShutdownGracePeriod).
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	sem := make(chan struct{}, w.cfg.Concurrency)

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runReaper(ctx)
	}()

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
		}

		free := w.cfg.Concurrency - len(sem)
		if free <= 0 {
			select {
			case <-ctx.Done():
				break loop
			case <-ticker.C:
				continue
			}
		}

		jobs, err := w.store.ClaimBatch(ctx, queue.ClaimParams{
			WorkerID:              w.cfg.WorkerID,
			Queue:                 w.cfg.Queue,
			Limit:                 free,
			LeaseTTL:              w.cfg.LeaseTTL,
			QueueConcurrencyLimit: w.cfg.QueueConcurrencyLimit,
			KeyConcurrencyLimit:   w.cfg.KeyConcurrencyLimit,
		})
		if err != nil {
			if ctx.Err() != nil {
				break loop
			}
			w.logger.Error("claim batch failed", "worker_id", w.cfg.WorkerID, "err", err)
			select {
			case <-ctx.Done():
				break loop
			case <-ticker.C:
			}
			continue
		}

		if len(jobs) == 0 {
			select {
			case <-ctx.Done():
				break loop
			case <-ticker.C:
			}
			continue
		}

		for _, job := range jobs {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				break loop
			}
			wg.Add(1)
			go func(j queue.Job) {
				defer wg.Done()
				defer func() { <-sem }()
				w.executeJob(ctx, j)
			}(job)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(w.cfg.ShutdownGracePeriod):
		return fmt.Errorf("worker %s shutdown timed out after %v", w.cfg.WorkerID, w.cfg.ShutdownGracePeriod)
	}
}

func (w *Worker) runReaper(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.ReaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := w.store.ReapExpired(ctx, 100, w.cfg.Backoff)
			if err != nil {
				if ctx.Err() == nil {
					w.logger.Error("reap expired failed", "worker_id", w.cfg.WorkerID, "err", err)
				}
				continue
			}
			if res.Requeued > 0 || res.DeadLettered > 0 {
				w.logger.Info("reaped expired leases",
					"worker_id", w.cfg.WorkerID,
					"requeued", res.Requeued,
					"dead_lettered", res.DeadLettered,
				)
			}
		}
	}
}

func (w *Worker) executeJob(parentCtx context.Context, job queue.Job) {
	if job.LeaseToken == nil {
		w.logger.Error("claimed job missing lease_token", "job_id", job.ID)
		return
	}
	leaseToken := *job.LeaseToken

	jobCtx, cancelJob := context.WithCancel(parentCtx)
	defer cancelJob()

	hbInterval := w.cfg.LeaseTTL / 3
	if hbInterval < 50*time.Millisecond {
		hbInterval = 50 * time.Millisecond
	}

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		ticker := time.NewTicker(hbInterval)
		defer ticker.Stop()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-ticker.C:
				ok, err := w.store.Heartbeat(jobCtx, job.ID, leaseToken, w.cfg.LeaseTTL)
				if err != nil {
					if jobCtx.Err() == nil {
						w.logger.Warn("heartbeat error", "job_id", job.ID, "err", err)
					}
					continue
				}
				if !ok {
					// Lost lease: self-fence immediately.
					w.logger.Warn("lost lease during heartbeat; cancelling job", "job_id", job.ID, "worker_id", w.cfg.WorkerID)
					cancelJob()
					return
				}
			}
		}
	}()

	handler, ok := w.handlerFor(job.JobType)
	var execErr error
	if !ok {
		execErr = NonRetryable(fmt.Errorf("no handler registered for job_type %q", job.JobType))
	} else {
		execErr = handler(jobCtx, job)
	}

	cancelJob()
	<-hbDone

	// Use a short background context for final state reporting so graceful shutdown doesn't drop completion.
	reportCtx, cancelReport := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReport()

	if execErr == nil {
		completed, err := w.store.Complete(reportCtx, job.ID, leaseToken)
		if err != nil {
			w.logger.Error("complete job failed", "job_id", job.ID, "err", err)
			return
		}
		if !completed {
			w.logger.Debug("complete skipped (fenced or already completed in handler tx)", "job_id", job.ID)
		}
		return
	}

	var nonRetry *NonRetryableError
	retryable := !errors.As(execErr, &nonRetry)
	outcome, err := w.store.Fail(reportCtx, job.ID, leaseToken, execErr.Error(), retryable, w.cfg.Backoff)
	if err != nil {
		w.logger.Error("fail job failed", "job_id", job.ID, "err", err)
		return
	}
	if outcome.FencedOut {
		w.logger.Warn("fail skipped because lease was lost", "job_id", job.ID)
	}
}
