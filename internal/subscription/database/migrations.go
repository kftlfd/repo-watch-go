package database

import (
	"context"
	"database/sql"
	"embed"
	"repo-watch/pkg/postgres"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func ApplyMigrations(ctx context.Context, db *sql.DB) error {
	return postgres.Migrate(ctx, db, migrationsFS)
}
