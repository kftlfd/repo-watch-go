package containers

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"repo-watch/internal/postgres"
)

const postgresImage = "postgres:16-alpine"

type Postgres struct {
	DB  *sql.DB
	DSN string
}

func NewPostgres(t *testing.T, dbName string) *Postgres {
	t.Helper()

	container, err := pg.Run(
		t.Context(),
		postgresImage,
		pg.WithDatabase(dbName),
		pg.WithUsername("postgres"),
		pg.WithPassword("postgres"),
		pg.BasicWaitStrategies(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(container))
	})

	dsn, err := container.ConnectionString(t.Context(), "sslmode=disable")
	require.NoError(t, err)

	db, err := postgres.Connect(t.Context(), postgres.Config{URL: dsn}, false, false, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})

	return &Postgres{
		DB:  db,
		DSN: dsn,
	}
}

func NewPostgresDB(t *testing.T, dbName string) *sql.DB {
	t.Helper()
	return NewPostgres(t, dbName).DB
}
