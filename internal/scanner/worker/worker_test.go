package worker

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"repo-watch/internal/github"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/scanner/database"
	"repo-watch/internal/scanner/database/migrations"
)

type scannerTestDB struct {
	Container *pg.PostgresContainer
	DB        *sql.DB
	Queries   *database.Queries
}

func newScannerTestDB(t *testing.T) *scannerTestDB {
	t.Helper()

	ctx := context.Background()

	container, err := pg.Run(
		ctx,
		"postgres:16-alpine",
		pg.WithDatabase("scanner_test"),
		pg.WithUsername("postgres"),
		pg.WithPassword("postgres"),
		pg.BasicWaitStrategies(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(container))
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := postgres.Connect(dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	require.NoError(t, postgres.Migrate(ctx, db, migrations.MigrationsFS))

	return &scannerTestDB{
		Container: container,
		DB:        db,
		Queries:   database.New(db),
	}
}

type fakeGithub struct {
	tag string
	err error
}

func (f *fakeGithub) GetLatestReleaseTag(context.Context, string, string) (string, error) {
	return f.tag, f.err
}

type funcGithub func(ctx context.Context, owner, repo string) (string, error)

func (f funcGithub) GetLatestReleaseTag(ctx context.Context, owner, repo string) (string, error) {
	return f(ctx, owner, repo)
}

// blockingGithub waits for the iteration deadline, then returns a result. This
// simulates an API call that completes just as the iteration budget runs out.
type blockingGithub struct {
	tag string
	err error
}

func (f *blockingGithub) GetLatestReleaseTag(ctx context.Context, _, _ string) (string, error) {
	<-ctx.Done()
	return f.tag, f.err
}

type fakeMetrics struct {
	ok   int
	errs int
}

func (m *fakeMetrics) ScanOk()  { m.ok++ }
func (m *fakeMetrics) ScanErr() { m.errs++ }

func newTestWorker(db *sql.DB, q *database.Queries, gh GithubClient, cfg Config) (*scannerWorker, *fakeMetrics) {
	metrics := &fakeMetrics{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	return NewScannerWorker(Deps{
		Cfg:          cfg,
		Logger:       logger,
		DB:           db,
		Queries:      q,
		Publisher:    nil,
		GithubClient: gh,
		Metrics:      metrics,
	}), metrics
}

func testConfig() Config {
	return Config{
		BatchSize:        10,
		Interval:         time.Second,
		IterationTimeout: 5 * time.Second,
		LeaseDuration:    30 * time.Second,

		GithubMaxAttempts: 3,
		GithubRetryBase:   10 * time.Millisecond,
	}
}

func resetScannerDB(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	for _, stmt := range []string{
		`DROP TRIGGER IF EXISTS fail_scanner_repo_update ON repositories`,
		`DROP TRIGGER IF EXISTS fail_scanner_outbox_commit ON outbox`,
		`DROP FUNCTION IF EXISTS fail_scanner_repo_update()`,
		`DROP FUNCTION IF EXISTS fail_scanner_outbox_commit()`,
		`TRUNCATE TABLE outbox, releases, repositories RESTART IDENTITY CASCADE`,
	} {
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}
}

func insertRepo(t *testing.T, ctx context.Context, q *database.Queries, repoID, tag string) {
	t.Helper()

	err := q.CreateOrUpdateRepo(ctx, database.CreateOrUpdateRepoParams{
		ID:            repoID,
		Owner:         "owner",
		Name:          "repository",
		LatestSeenTag: tag,
		Active:        true,
		ActiveVersion: 1,
	})
	require.NoError(t, err)
}

func getRepoState(t *testing.T, ctx context.Context, db *sql.DB, repoID string) (tag string, active bool) {
	t.Helper()

	require.NoError(t, db.QueryRowContext(
		ctx,
		`SELECT latest_seen_tag, active FROM repositories WHERE id = $1`,
		repoID,
	).Scan(&tag, &active))

	return tag, active
}

func installRepoUpdateFailure(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION fail_scanner_repo_update() RETURNS trigger AS $$
		BEGIN
			IF NEW.last_checked_at IS DISTINCT FROM OLD.last_checked_at
				OR (OLD.active = true AND NEW.active = false) THEN
				RAISE EXCEPTION 'forced scanner repository update failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TRIGGER fail_scanner_repo_update
		BEFORE UPDATE ON repositories
		FOR EACH ROW EXECUTE FUNCTION fail_scanner_repo_update();
	`)
	require.NoError(t, err)
}

func installOutboxCommitFailure(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION fail_scanner_outbox_commit() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'forced scanner outbox commit failure';
		END;
		$$ LANGUAGE plpgsql;
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE CONSTRAINT TRIGGER fail_scanner_outbox_commit
		AFTER INSERT ON outbox
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION fail_scanner_outbox_commit();
	`)
	require.NoError(t, err)
}

func TestScannerWorkerTransactionFailures(t *testing.T) {
	db := newScannerTestDB(t)

	ctx := context.Background()
	repoID := "owner/repository"

	t.Run("repo update failure is returned and retryable after lease expiry", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")
		installRepoUpdateFailure(t, ctx, db.DB)

		// Keep the tag unchanged so MarkRepoProcessed is the only write in the
		// transaction. This catches the previous behavior where its error was
		// ignored and the transaction could still be reported as successful.
		gh := &fakeGithub{tag: "v1.0.0"}
		w, metrics := newTestWorker(db.DB, db.Queries, gh, testConfig())

		err := w.DoScanIteration()
		require.ErrorContains(t, err, "mark repo processed")
		require.Equal(t, 0, metrics.ok)

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.True(t, active)

		// Remove the failure, trigger a release on retry, and make the lease
		// immediately available again.
		_, err = db.DB.ExecContext(ctx, `DROP TRIGGER fail_scanner_repo_update ON repositories`)
		require.NoError(t, err)
		_, err = db.DB.ExecContext(
			ctx,
			`UPDATE repositories SET leased_until = now() - interval '1 second' WHERE id = $1`,
			repoID,
		)
		require.NoError(t, err)
		gh.tag = "v2.0.0"

		require.NoError(t, w.DoScanIteration())
		require.Equal(t, 1, metrics.ok)

		tag, active = getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v2.0.0", tag)
		require.True(t, active)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventReleaseDetected)
		require.NoError(t, err)
		require.EqualValues(t, 1, eventCount)
	})

	t.Run("repo update commit failure rolls back state", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")
		installOutboxCommitFailure(t, ctx, db.DB)

		gh := &fakeGithub{tag: "v2.0.0"}
		w, metrics := newTestWorker(db.DB, db.Queries, gh, testConfig())

		err := w.DoScanIteration()
		require.ErrorContains(t, err, "commit repo update")
		require.Equal(t, 0, metrics.ok)

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.True(t, active)

		var releaseCount int
		require.NoError(t, db.DB.QueryRowContext(ctx, `SELECT count(*) FROM releases`).Scan(&releaseCount))
		require.Zero(t, releaseCount)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventReleaseDetected)
		require.NoError(t, err)
		require.Zero(t, eventCount)
	})

	t.Run("repo removal failure is returned", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")
		installRepoUpdateFailure(t, ctx, db.DB)

		gh := &fakeGithub{err: github.ErrNotFound}
		w, _ := newTestWorker(db.DB, db.Queries, gh, testConfig())

		err := w.DoScanIteration()
		require.ErrorContains(t, err, "mark repo inactive")

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.True(t, active)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventRepositoryRemoved)
		require.NoError(t, err)
		require.Zero(t, eventCount)
	})

	t.Run("repo removal commit failure rolls back state", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")
		installOutboxCommitFailure(t, ctx, db.DB)

		gh := &fakeGithub{err: github.ErrNotFound}
		w, _ := newTestWorker(db.DB, db.Queries, gh, testConfig())

		err := w.DoScanIteration()
		require.ErrorContains(t, err, "commit repo removal")

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.True(t, active)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventRepositoryRemoved)
		require.NoError(t, err)
		require.Zero(t, eventCount)
	})

	t.Run("repo update persists after iteration deadline", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		cfg := testConfig()
		cfg.IterationTimeout = 200 * time.Millisecond

		gh := &blockingGithub{tag: "v2.0.0"}
		w, metrics := newTestWorker(db.DB, db.Queries, gh, cfg)

		// The GitHub call returns successfully only after the iteration context
		// has expired. Persistence must still commit.
		require.NoError(t, w.DoScanIteration())
		require.Equal(t, 1, metrics.ok)

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v2.0.0", tag)
		require.True(t, active)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventReleaseDetected)
		require.NoError(t, err)
		require.EqualValues(t, 1, eventCount)
	})

	t.Run("repo removal persists after iteration deadline", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		cfg := testConfig()
		cfg.IterationTimeout = 200 * time.Millisecond

		gh := &blockingGithub{err: github.ErrNotFound}
		w, _ := newTestWorker(db.DB, db.Queries, gh, cfg)

		require.NoError(t, w.DoScanIteration())

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.False(t, active)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventRepositoryRemoved)
		require.NoError(t, err)
		require.EqualValues(t, 1, eventCount)
	})
}

func TestScannerWorkerGithubRetries(t *testing.T) {
	db := newScannerTestDB(t)

	ctx := context.Background()
	repoID := "owner/repository"

	t.Run("transient error is retried and processed", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		calls := 0
		gh := funcGithub(func(context.Context, string, string) (string, error) {
			calls++
			if calls == 1 {
				return "", fmt.Errorf("%w: status 502", github.ErrTransientError)
			}
			return "v2.0.0", nil
		})
		w, metrics := newTestWorker(db.DB, db.Queries, gh, testConfig())

		require.NoError(t, w.DoScanIteration())
		require.Equal(t, 2, calls)
		require.Equal(t, 1, metrics.ok)

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v2.0.0", tag)
		require.True(t, active)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventReleaseDetected)
		require.NoError(t, err)
		require.EqualValues(t, 1, eventCount)
	})

	t.Run("rate limit with retry hint is retried", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		calls := 0
		gh := funcGithub(func(context.Context, string, string) (string, error) {
			calls++
			if calls == 1 {
				return "", &github.RateLimitError{
					StatusCode: 429,
					RetryAfter: 10 * time.Millisecond,
				}
			}
			return "v2.0.0", nil
		})
		w, metrics := newTestWorker(db.DB, db.Queries, gh, testConfig())

		require.NoError(t, w.DoScanIteration())
		require.Equal(t, 2, calls)
		require.Equal(t, 1, metrics.ok)

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v2.0.0", tag)
		require.True(t, active)
	})

	t.Run("rate limit without hint leaves repo for lease expiry", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		calls := 0
		gh := funcGithub(func(context.Context, string, string) (string, error) {
			calls++
			return "", &github.RateLimitError{StatusCode: 403}
		})
		w, metrics := newTestWorker(db.DB, db.Queries, gh, testConfig())

		require.NoError(t, w.DoScanIteration())
		require.Equal(t, 1, calls)
		require.Zero(t, metrics.ok)

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.True(t, active)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventReleaseDetected)
		require.NoError(t, err)
		require.Zero(t, eventCount)

		// The repo lease is still active, so an immediate second iteration must
		// not call GitHub again.
		require.NoError(t, w.DoScanIteration())
		require.Equal(t, 1, calls)
	})

	t.Run("rate limit stops the rest of the batch", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, "owner/first", "v1.0.0")
		insertRepo(t, ctx, db.Queries, "owner/second", "v1.0.0")

		_, err := db.DB.ExecContext(
			ctx,
			`UPDATE repositories SET last_checked_at = now() - interval '1 hour' WHERE id = 'owner/first'`,
		)
		require.NoError(t, err)

		calls := 0
		gh := funcGithub(func(context.Context, string, string) (string, error) {
			calls++
			return "", &github.RateLimitError{StatusCode: 429}
		})
		w, _ := newTestWorker(db.DB, db.Queries, gh, testConfig())

		require.NoError(t, w.DoScanIteration())
		require.Equal(t, 1, calls)

		for _, id := range []string{"owner/first", "owner/second"} {
			tag, active := getRepoState(t, ctx, db.DB, id)
			require.Equal(t, "v1.0.0", tag)
			require.True(t, active)
		}

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventReleaseDetected)
		require.NoError(t, err)
		require.Zero(t, eventCount)
	})

	t.Run("exhausted transient errors do not mark the repo processed", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		calls := 0
		gh := funcGithub(func(context.Context, string, string) (string, error) {
			calls++
			return "", fmt.Errorf("%w: status 503", github.ErrTransientError)
		})
		w, metrics := newTestWorker(db.DB, db.Queries, gh, testConfig())

		require.NoError(t, w.DoScanIteration())
		require.Equal(t, 3, calls)
		require.Zero(t, metrics.ok)

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.True(t, active)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventReleaseDetected)
		require.NoError(t, err)
		require.Zero(t, eventCount)
	})
}
