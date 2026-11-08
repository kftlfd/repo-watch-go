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

func NewPostgresDB(t *testing.T, dbName string) *sql.DB {
	t.Helper()

	container, err := pg.Run(
		t.Context(),
		postgresImage,
		pg.WithDatabase("subscription_test"),
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

	db, err := postgres.Connect(dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	return db
}
