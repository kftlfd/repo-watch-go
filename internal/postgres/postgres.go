package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"

	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

func open(url string) (*sql.DB, error) {
	conn, err := sql.Open("postgres", url)
	return conn, err
}

// EnsureDbExists creates dbName using the maintenance database if it does not
// already exist. Concurrent creations are treated as success.
func EnsureDbExists(ctx context.Context, maintenanceURL, dbName string) error {
	db, err := open(maintenanceURL)
	if err != nil {
		return fmt.Errorf("connect maintenance database: %w", err)
	}
	defer func() { _ = db.Close() }()

	exists, err := databaseExists(ctx, db, dbName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	_, err = db.ExecContext(ctx, "CREATE DATABASE "+pq.QuoteIdentifier(dbName))
	if err == nil {
		return nil
	}

	if pqErr, ok := errors.AsType[*pq.Error](err); ok && pqErr.Code == "42P04" {
		// Another instance created the database first.
		return nil
	}

	// A concurrent create can also surface as a different error. Re-check
	// before reporting failure.
	if exists, checkErr := databaseExists(ctx, db, dbName); checkErr == nil && exists {
		return nil
	}

	return fmt.Errorf("create database %q: %w", dbName, err)
}

func databaseExists(ctx context.Context, db *sql.DB, dbName string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(
		ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM pg_database
			WHERE datname = $1
		)`,
		dbName,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check database %q: %w", dbName, err)
	}

	return exists, nil
}

func Migrate(ctx context.Context, db *sql.DB, migrationsFS fs.FS) error {
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("create migration locker: %w", err)
	}

	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		migrationsFS,
		goose.WithSessionLocker(locker),
	)
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}

	_, err = provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	return nil
}
