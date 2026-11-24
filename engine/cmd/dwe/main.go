package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/aaron/durable-workflow-engine/engine/pkg/dwe"
	"github.com/google/uuid"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "dwe error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		printUsage()
		return fmt.Errorf("subcommand required")
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://dwe:dwe@localhost:5432/dwe?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := dwe.Open(ctx, dbURL)
	if err != nil {
		return err
	}
	defer client.Close()

	switch os.Args[1] {
	case "migrate":
		if err := client.Migrate(ctx); err != nil {
			return err
		}
		fmt.Println("migrations applied")
		return nil

	case "enqueue":
		fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
		q := fs.String("queue", "default", "Queue name")
		jobType := fs.String("type", "", "Job type (required)")
		payload := fs.String("payload", "{}", "JSON payload")
		idemp := fs.String("idempotency-key", "", "Optional idempotency key")
		concKey := fs.String("concurrency-key", "", "Optional concurrency key")
		priority := fs.Int("priority", 0, "Priority (higher runs first)")
		maxAttempts := fs.Int("max-attempts", 5, "Max attempts")
		if err := fs.Parse(os.Args[2:]); err != nil {
			return err
		}
		if *jobType == "" {
			return fmt.Errorf("--type is required")
		}
		job, dedup, err := client.Enqueue(ctx, dwe.EnqueueParams{
			Queue:          *q,
			JobType:        *jobType,
			Payload:        json.RawMessage(*payload),
			IdempotencyKey: *idemp,
			ConcurrencyKey: *concKey,
			Priority:       int16(*priority),
			MaxAttempts:    *maxAttempts,
		})
		if err != nil {
			return err
		}
		fmt.Printf("job_id=%s queue=%s status=%s deduplicated=%v\n", job.ID, job.Queue, job.Status, dedup)
		return nil

	case "status":
		stats, err := client.Stats(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "QUEUE\tQUEUED\tRUNNING\tCOMPLETED\tDEAD\tCANCELLED")
		for _, s := range stats {
			fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\n", s.Queue, s.Queued, s.Running, s.Completed, s.Dead, s.Cancelled)
		}
		return tw.Flush()

	case "dlq":
		if len(os.Args) < 3 {
			return fmt.Errorf("dlq subcommand required: list | replay <id>")
		}
		switch os.Args[2] {
		case "list":
			fs := flag.NewFlagSet("dlq list", flag.ExitOnError)
			q := fs.String("queue", "", "Filter by queue")
			limit := fs.Int("limit", 50, "Max rows")
			if err := fs.Parse(os.Args[3:]); err != nil {
				return err
			}
			items, err := client.ListDLQ(ctx, *q, *limit)
			if err != nil {
				return err
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "DLQ_ID\tQUEUE\tJOB_TYPE\tATTEMPTS\tREASON\tREPLAYED_JOB_ID")
			for _, d := range items {
				replayed := "-"
				if d.ReplayedJobID != nil {
					replayed = d.ReplayedJobID.String()
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n", d.ID, d.Queue, d.JobType, d.Attempts, d.FailureReason, replayed)
			}
			return tw.Flush()

		case "replay":
			if len(os.Args) < 4 {
				return fmt.Errorf("dlq replay <dlq-id> required")
			}
			id, err := uuid.Parse(os.Args[3])
			if err != nil {
				return fmt.Errorf("invalid dlq id: %w", err)
			}
			job, err := client.ReplayDLQ(ctx, id)
			if err != nil {
				return err
			}
			fmt.Printf("replayed dlq_id=%s -> new_job_id=%s\n", id, job.ID)
			return nil
		default:
			return fmt.Errorf("unknown dlq subcommand %q", os.Args[2])
		}

	default:
		printUsage()
		return fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: dwe <migrate|enqueue|status|dlq list|dlq replay <id>>")
}
