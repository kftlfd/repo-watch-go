package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

func CreateDbUrl(baseDbUrl, dbName string) string {
	return baseDbUrl + "/" + dbName + "?sslmode=disable"
}

func Connect(url string) (*sql.DB, error) {
	conn, err := sql.Open("postgres", url)
	return conn, err
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
	defer provider.Close()

	_, err = provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	return nil
}
