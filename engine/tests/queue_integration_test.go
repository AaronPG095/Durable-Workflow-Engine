package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aaron/durable-workflow-engine/engine/internal/db"
	"github.com/aaron/durable-workflow-engine/engine/internal/queue"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func openTestDB(t *testing.T) (*pgxpool.Pool, *queue.Store) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	ctx := context.Background()
	pool, err := db.OpenPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE dead_letter_jobs, workflow_signals, workflow_steps, jobs, workflow_runs CASCADE`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	return pool, queue.NewStore(pool)
}

func TestEnqueueIdempotency(t *testing.T) {
	_, store := openTestDB(t)
	ctx := context.Background()

	j1, dedup1, err := store.Enqueue(ctx, queue.EnqueueParams{
		Queue:          "emails",
		JobType:        "send_welcome",
		Payload:        json.RawMessage(`{"user":1}`),
		IdempotencyKey: "welcome:user:1",
	})
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if dedup1 {
		t.Fatalf("expected first enqueue not deduplicated")
	}

	j2, dedup2, err := store.Enqueue(ctx, queue.EnqueueParams{
		Queue:          "emails",
		JobType:        "send_welcome",
		Payload:        json.RawMessage(`{"user":1}`),
		IdempotencyKey: "welcome:user:1",
	})
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if !dedup2 || j1.ID != j2.ID {
		t.Fatalf("expected deduplicated job %s, got %s (dedup=%v)", j1.ID, j2.ID, dedup2)
	}
}

func TestMultiWorkerSkipLockedNoDoubleClaim(t *testing.T) {
	_, store := openTestDB(t)
	ctx := context.Background()

	const total = 40
	for i := 0; i < total; i++ {
		if _, _, err := store.Enqueue(ctx, queue.EnqueueParams{
			Queue:   "batch",
			JobType: "noop",
			Payload: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	claimed := make(map[uuid.UUID]string)
	var wg sync.WaitGroup

	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				jobs, err := store.ClaimBatch(ctx, queue.ClaimParams{
					WorkerID: workerID,
					Queue:    "batch",
					Limit:    3,
					LeaseTTL: 10 * time.Second,
				})
				if err != nil {
					t.Errorf("claim error: %v", err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					if prev, exists := claimed[j.ID]; exists {
						t.Errorf("double claim on job %s by %s and %s", j.ID, prev, workerID)
					}
					claimed[j.ID] = workerID
				}
				mu.Unlock()
			}
		}(fmt.Sprintf("worker-%d", w))
	}
	wg.Wait()

	if len(claimed) != total {
		t.Fatalf("expected %d unique claims, got %d", total, len(claimed))
	}
}

func TestFencingTokenRejectsStaleWorker(t *testing.T) {
	pool, store := openTestDB(t)
	ctx := context.Background()

	j, _, err := store.Enqueue(ctx, queue.EnqueueParams{
		Queue:       "fence",
		JobType:     "work",
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	c1, err := store.ClaimBatch(ctx, queue.ClaimParams{
		WorkerID: "worker-1",
		Queue:    "fence",
		Limit:    1,
		LeaseTTL: 5 * time.Second,
	})
	if err != nil || len(c1) != 1 {
		t.Fatalf("claim 1 failed: %v len=%d", err, len(c1))
	}
	staleToken := *c1[0].LeaseToken

	// Force lease expiry and reap so worker-2 can claim with a new lease_token.
	if _, err := pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, j.ID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	reaped, err := store.ReapExpired(ctx, 10, queue.BackoffConfig{BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	if err != nil || reaped.Requeued != 1 {
		t.Fatalf("reap failed: %+v err=%v", reaped, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE jobs SET run_at = now() WHERE id = $1`, j.ID); err != nil {
		t.Fatalf("reset run_at: %v", err)
	}

	c2, err := store.ClaimBatch(ctx, queue.ClaimParams{
		WorkerID: "worker-2",
		Queue:    "fence",
		Limit:    1,
		LeaseTTL: 5 * time.Second,
	})
	if err != nil || len(c2) != 1 {
		t.Fatalf("claim 2 failed: %v len=%d", err, len(c2))
	}

	if ok, err := store.Heartbeat(ctx, j.ID, staleToken, 5*time.Second); err != nil || ok {
		t.Fatalf("expected stale heartbeat to fail, ok=%v err=%v", ok, err)
	}
	if ok, err := store.Complete(ctx, j.ID, staleToken); err != nil || ok {
		t.Fatalf("expected stale complete to fail, ok=%v err=%v", ok, err)
	}
	outcome, err := store.Fail(ctx, j.ID, staleToken, "stale fail", true, queue.DefaultBackoffConfig())
	if err != nil || !outcome.FencedOut {
		t.Fatalf("expected stale fail to be fenced out, outcome=%+v err=%v", outcome, err)
	}

	// Active worker-2 can still complete.
	if ok, err := store.Complete(ctx, j.ID, *c2[0].LeaseToken); err != nil || !ok {
		t.Fatalf("expected active worker complete to succeed, ok=%v err=%v", ok, err)
	}
}

func TestConcurrencyLimits(t *testing.T) {
	_, store := openTestDB(t)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		if _, _, err := store.Enqueue(ctx, queue.EnqueueParams{
			Queue:          "limited",
			JobType:        "work",
			ConcurrencyKey: "repo:alpha",
		}); err != nil {
			t.Fatalf("enqueue alpha %d: %v", i, err)
		}
	}
	if _, _, err := store.Enqueue(ctx, queue.EnqueueParams{
		Queue:          "limited",
		JobType:        "work",
		ConcurrencyKey: "repo:beta",
	}); err != nil {
		t.Fatalf("enqueue beta: %v", err)
	}

	claimed, err := store.ClaimBatch(ctx, queue.ClaimParams{
		WorkerID:              "w1",
		Queue:                 "limited",
		Limit:                 5,
		LeaseTTL:              10 * time.Second,
		QueueConcurrencyLimit: 2,
		KeyConcurrencyLimit:   1,
	})
	if err != nil {
		t.Fatalf("claim with limits: %v", err)
	}
	// Max 1 from repo:alpha + 1 from repo:beta = 2 total (also hits queue limit 2).
	if len(claimed) != 2 {
		t.Fatalf("expected 2 claimed respecting key+queue limits, got %d", len(claimed))
	}
}

func TestDLQTransitionAndAtomicReplay(t *testing.T) {
	_, store := openTestDB(t)
	ctx := context.Background()

	_, _, err := store.Enqueue(ctx, queue.EnqueueParams{
		Queue:       "dlq_test",
		JobType:     "boom",
		Payload:     json.RawMessage(`{"x":1}`),
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claimed, err := store.ClaimBatch(ctx, queue.ClaimParams{WorkerID: "w1", Queue: "dlq_test", Limit: 1, LeaseTTL: 5 * time.Second})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v", err)
	}

	outcome, err := store.Fail(ctx, claimed[0].ID, *claimed[0].LeaseToken, "fatal bug", false, queue.DefaultBackoffConfig())
	if err != nil || !outcome.DeadLettered {
		t.Fatalf("expected immediate DLQ on non-retryable failure, got %+v err=%v", outcome, err)
	}

	dlqItems, err := store.ListDLQ(ctx, "dlq_test", 10)
	if err != nil || len(dlqItems) != 1 {
		t.Fatalf("expected 1 DLQ item, got %d err=%v", len(dlqItems), err)
	}

	replayed, err := store.ReplayDLQ(ctx, dlqItems[0].ID)
	if err != nil {
		t.Fatalf("replay DLQ: %v", err)
	}
	if replayed.Status != queue.StatusQueued {
		t.Fatalf("expected replayed job queued, got %s", replayed.Status)
	}

	if _, err := store.ReplayDLQ(ctx, dlqItems[0].ID); !errors.Is(err, queue.ErrAlreadyReplayed) {
		t.Fatalf("expected ErrAlreadyReplayed on second replay, got %v", err)
	}
}
