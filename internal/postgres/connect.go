package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
)

const defaultMaintenanceDB = "postgres"

// Connect opens a connection to the configured database.
//
// When ensureExists is true, the database is created using the maintenance
// database if it does not exist. When applyMigrations is true, migrations from
// migrationsFS are applied before the connection is returned.
//
// The caller owns the returned connection and must close it.
func Connect(
	ctx context.Context,
	cfg Config,
	ensureExists bool,
	applyMigrations bool,
	migrationsFS fs.FS,
) (*sql.DB, error) {
	if applyMigrations && migrationsFS == nil {
		return nil, errors.New("migrations filesystem is required")
	}

	url, err := cfg.Resolve()
	if err != nil {
		return nil, err
	}

	pool := cfg.Pool.withDefaults()
	if applyMigrations && pool.MaxOpenConns < 2 {
		return nil, errors.New("DB pool must allow at least 2 connections when applying migrations")
	}

	if ensureExists {
		maintenanceURL, dbName, err := maintenanceInfo(url)
		if err != nil {
			return nil, fmt.Errorf("parse DB URL: %w", err)
		}
		if err := EnsureDbExists(ctx, maintenanceURL, dbName); err != nil {
			return nil, fmt.Errorf("ensure DB exists: %w", err)
		}
	}

	db, err := open(url)
	if err != nil {
		return nil, fmt.Errorf("DB connect: %w", err)
	}

	pool.apply(db)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("DB ping: %w", err)
	}

	if applyMigrations {
		if err := Migrate(ctx, db, migrationsFS); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("DB migrate: %w", err)
		}
	}

	return db, nil
}

// maintenanceInfo derives the maintenance database URL and database name from
// a database URL.
func maintenanceInfo(rawURL string) (string, string, error) {
	u, err := parseURL(rawURL)
	if err != nil {
		return "", "", err
	}

	dbName, err := resolveDatabaseName(u, "")
	if err != nil {
		return "", "", err
	}

	maintenance := *u
	maintenance.Path = "/" + defaultMaintenanceDB
	maintenance.RawPath = ""
	maintenance.Fragment = ""

	q := maintenance.Query()
	q.Del("dbname")
	maintenance.RawQuery = q.Encode()

	return maintenance.String(), dbName, nil
}
