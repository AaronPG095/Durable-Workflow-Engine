package db

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/aaron/durable-workflow-engine/engine/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Migrate applies all embedded SQL migrations idempotently inside transactions.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, name := range files {
		if err := applyMigration(ctx, pool, name); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, pool *pgxpool.Pool, version string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize concurrent migrators on startup without blocking unrelated queries.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('dwe_schema_migrations'))`); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	var alreadyApplied bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&alreadyApplied); err != nil {
		return fmt.Errorf("check migration %s: %w", version, err)
	}
	if alreadyApplied {
		return tx.Commit(ctx)
	}

	sqlBytes, err := fs.ReadFile(migrations.FS, version)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", version, err)
	}

	if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
		return fmt.Errorf("execute migration %s: %w", version, err)
	}

	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
		return fmt.Errorf("record migration %s: %w", version, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", version, err)
	}
	return nil
}
