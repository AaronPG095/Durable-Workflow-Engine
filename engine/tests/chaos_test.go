package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aaron/durable-workflow-engine/engine/internal/db"
	"github.com/aaron/durable-workflow-engine/engine/internal/queue"
)

func TestChaosSIGKILLWorkersZeroLostZeroDuplicates(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set; skipping multi-process SIGKILL chaos test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool, err := db.OpenPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS chaos_effects (
			item_id     TEXT PRIMARY KEY,
			job_id      UUID NOT NULL,
			worker_id   TEXT NOT NULL,
			executed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS chaos_attempts (
			id          BIGSERIAL PRIMARY KEY,
			job_id      UUID NOT NULL,
			item_id     TEXT NOT NULL,
			worker_id   TEXT NOT NULL,
			attempt     INT NOT NULL,
			started_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		TRUNCATE TABLE dead_letter_jobs, workflow_signals, workflow_steps, jobs, workflow_runs, chaos_effects, chaos_attempts CASCADE;
	`); err != nil {
		t.Fatalf("setup chaos tables: %v", err)
	}

	// Build dwe-worker binary into a temp directory.
	binName := "dwe-worker"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	workerBin := filepath.Join(t.TempDir(), binName)
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", workerBin, "../cmd/dwe-worker")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build dwe-worker: %v\n%s", err, string(out))
	}

	store := queue.NewStore(pool)
	const totalJobs = 60
	for i := 0; i < totalJobs; i++ {
		payload, _ := json.Marshal(map[string]any{
			"item_id":     fmt.Sprintf("item-%03d", i),
			"work_ms_min": 250,
			"work_ms_max": 600,
		})
		if _, _, err := store.Enqueue(ctx, queue.EnqueueParams{
			Queue:          "chaos",
			JobType:        "chaos_work",
			Payload:        payload,
			IdempotencyKey: fmt.Sprintf("chaos:%03d", i),
			MaxAttempts:    10,
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	procs := make(map[int]*exec.Cmd)
	nextID := 0
	spawnWorker := func() int {
		mu.Lock()
		nextID++
		id := nextID
		mu.Unlock()

		workerName := fmt.Sprintf("chaos-w-%d", id)
		cmd := exec.Command(workerBin,
			"--database-url", dbURL,
			"--queue", "chaos",
			"--worker-id", workerName,
			"--concurrency", "4",
			"--poll-interval", "50ms",
			"--lease-ttl", "2s",
			"--reaper-interval", "400ms",
			"--chaos-mode",
		)
		if err := cmd.Start(); err != nil {
			t.Fatalf("start worker %s: %v", workerName, err)
		}
		mu.Lock()
		procs[id] = cmd
		mu.Unlock()
		return id
	}

	killAll := func() {
		mu.Lock()
		defer mu.Unlock()
		for id, cmd := range procs {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			_ = cmd.Wait()
			delete(procs, id)
		}
	}
	defer killAll()

	// Start 4 concurrent worker OS processes.
	activeIDs := make([]int, 0, 4)
	for i := 0; i < 4; i++ {
		activeIDs = append(activeIDs, spawnWorker())
	}

	// Chaos killer loop: randomly SIGKILL workers mid-flight and respawn replacements.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	stopChaos := make(chan struct{})
	chaosDone := make(chan struct{})
	kills := 0
	go func() {
		defer close(chaosDone)
		for {
			select {
			case <-stopChaos:
				return
			case <-time.After(900 * time.Millisecond):
				slot := rng.Intn(len(activeIDs))
				victimID := activeIDs[slot]
				mu.Lock()
				cmd := procs[victimID]
				if cmd != nil && cmd.Process != nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					delete(procs, victimID)
					kills++
				}
				mu.Unlock()
				activeIDs[slot] = spawnWorker()
			}
		}
	}()

	// Wait until all 60 jobs complete or timeout.
	deadline := time.Now().Add(60 * time.Second)
	for {
		var completed, dead int
		if err := pool.QueryRow(ctx, `
			SELECT
				count(*) FILTER (WHERE status = 'completed'),
				count(*) FILTER (WHERE status = 'dead')
			FROM jobs WHERE queue = 'chaos'
		`).Scan(&completed, &dead); err != nil {
			close(stopChaos)
			<-chaosDone
			t.Fatalf("poll completion: %v", err)
		}
		if dead > 0 {
			close(stopChaos)
			<-chaosDone
			t.Fatalf("unexpected dead-lettered jobs: %d", dead)
		}
		if completed == totalJobs {
			break
		}
		if time.Now().After(deadline) {
			close(stopChaos)
			<-chaosDone
			t.Fatalf("timed out waiting for chaos jobs: completed=%d/%d", completed, totalJobs)
		}
		time.Sleep(250 * time.Millisecond)
	}

	close(stopChaos)
	<-chaosDone
	killAll()

	var effectCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM chaos_effects`).Scan(&effectCount); err != nil {
		t.Fatalf("count chaos_effects: %v", err)
	}
	if effectCount != totalJobs {
		t.Fatalf("expected exactly %d unique side effects, got %d", totalJobs, effectCount)
	}

	var recoveredJobs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue = 'chaos' AND attempt > 1`).Scan(&recoveredJobs); err != nil {
		t.Fatalf("count recovered jobs: %v", err)
	}
	t.Logf("chaos test passed: jobs=%d effects=%d worker_kills=%d recovered_after_kill=%d", totalJobs, effectCount, kills, recoveredJobs)
}
