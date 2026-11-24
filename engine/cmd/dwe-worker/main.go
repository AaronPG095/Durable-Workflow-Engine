package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aaron/durable-workflow-engine/engine/pkg/dwe"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "dwe-worker error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	dbURL := flag.String("database-url", envOr("DATABASE_URL", "postgres://dwe:dwe@localhost:5432/dwe?sslmode=disable"), "PostgreSQL connection URL")
	queueName := flag.String("queue", envOr("DWE_QUEUE", "default"), "Queue name")
	workerID := flag.String("worker-id", envOr("DWE_WORKER_ID", ""), "Worker identifier")
	concurrency := flag.Int("concurrency", envInt("DWE_CONCURRENCY", 4), "Max concurrent jobs")
	pollInterval := flag.Duration("poll-interval", envDur("DWE_POLL_INTERVAL", 100*time.Millisecond), "Poll interval")
	leaseTTL := flag.Duration("lease-ttl", envDur("DWE_LEASE_TTL", 15*time.Second), "Lease TTL")
	reaperInterval := flag.Duration("reaper-interval", envDur("DWE_REAPER_INTERVAL", 2*time.Second), "Reaper sweep interval")
	chaosMode := flag.Bool("chaos-mode", os.Getenv("DWE_CHAOS_MODE") == "1", "Enable chaos_work handler for SIGKILL testing")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := dwe.Open(ctx, *dbURL)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := client.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	w := client.NewWorker(dwe.WorkerConfig{
		WorkerID:       *workerID,
		Queue:          *queueName,
		Concurrency:    *concurrency,
		PollInterval:   *pollInterval,
		LeaseTTL:       *leaseTTL,
		ReaperInterval: *reaperInterval,
		Logger:         logger,
	})

	w.Register("echo", func(ctx context.Context, job dwe.Job) error {
		logger.Info("processed echo job", "job_id", job.ID, "attempt", job.Attempt, "payload", string(job.Payload))
		return nil
	})

	if *chaosMode {
		registerChaosHandler(w, client, *workerID)
	}

	logger.Info("worker starting", "queue", *queueName, "concurrency", *concurrency, "lease_ttl", leaseTTL.String(), "chaos_mode", *chaosMode)
	return w.Run(ctx)
}

func registerChaosHandler(w interface {
	Register(string, dwe.HandlerFunc)
}, client *dwe.Client, workerID string) {
	w.Register("chaos_work", func(ctx context.Context, job dwe.Job) error {
		var p struct {
			ItemID    string `json:"item_id"`
			WorkMsMin int    `json:"work_ms_min"`
			WorkMsMax int    `json:"work_ms_max"`
		}
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			return dwe.NonRetryable(err)
		}
		if p.WorkMsMin <= 0 {
			p.WorkMsMin = 250
		}
		if p.WorkMsMax < p.WorkMsMin {
			p.WorkMsMax = p.WorkMsMin
		}

		// Record every attempt in an audit table (best-effort) to observe recovery attempts.
		_, _ = client.Pool().Exec(ctx,
			`INSERT INTO chaos_attempts (job_id, item_id, worker_id, attempt) VALUES ($1, $2, $3, $4)`,
			job.ID, p.ItemID, workerID, job.Attempt,
		)

		delay := p.WorkMsMin
		if p.WorkMsMax > p.WorkMsMin {
			delay += rand.Intn(p.WorkMsMax - p.WorkMsMin + 1)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(delay) * time.Millisecond):
		}

		if job.LeaseToken == nil {
			return fmt.Errorf("missing lease token")
		}

		tx, err := client.Pool().Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// Atomic side-effect write + fenced job completion in a single transaction.
		completed, err := client.CompleteWithTx(ctx, tx, job.ID, *job.LeaseToken)
		if err != nil {
			return err
		}
		if !completed {
			return fmt.Errorf("fenced out before committing side effect")
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO chaos_effects (item_id, job_id, worker_id) VALUES ($1, $2, $3)`,
			p.ItemID, job.ID, workerID,
		); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envDur(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
