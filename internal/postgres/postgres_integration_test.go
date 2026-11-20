//go:build integration

package postgres_test

import (
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/postgres"
	"repo-watch/tests/containers"
)

func TestEnsureDbExistsConcurrent(t *testing.T) {
	container := containers.NewPostgres(t, "postgres")
	ctx := t.Context()

	const dbName = "repo-watch-test-db"

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for range workers {
		wg.Go(func() {
			errs <- postgres.EnsureDbExists(ctx, container.DSN, dbName)
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	target, err := url.Parse(container.DSN)
	require.NoError(t, err)
	target.Path = "/" + dbName
	target.RawPath = ""

	db, err := postgres.Connect(ctx, postgres.Config{URL: target.String()}, false, false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, db.PingContext(ctx))
}
