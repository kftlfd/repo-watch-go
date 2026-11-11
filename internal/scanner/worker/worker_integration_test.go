//go:build integration

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

	"repo-watch/internal/github"
	"repo-watch/internal/postgres"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/scanner/db/dbGen"
	"repo-watch/internal/scanner/db/migrations"
	"repo-watch/tests/containers"
)

type scannerTestDB struct {
	DB      *sql.DB
	Queries *dbGen.Queries
}

func newScannerTestDB(t *testing.T) *scannerTestDB {
	t.Helper()

	db := containers.NewPostgresDB(t, "scanner_test")
	require.NoError(t, postgres.Migrate(t.Context(), db, migrations.MigrationsFS))

	return &scannerTestDB{
		DB:      db,
		Queries: dbGen.New(db),
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

func newTestWorker(db *sql.DB, q *dbGen.Queries, gh GithubClient, cfg Config) (*scannerWorker, *fakeMetrics) {
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

func insertRepo(t *testing.T, ctx context.Context, q *dbGen.Queries, repoID, tag string) {
	t.Helper()

	err := q.CreateOrUpdateRepo(ctx, dbGen.CreateOrUpdateRepoParams{
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

func getRepoActiveVersion(t *testing.T, ctx context.Context, db *sql.DB, repoID string) int32 {
	t.Helper()

	var version int32
	require.NoError(t, db.QueryRowContext(
		ctx,
		`SELECT active_version FROM repositories WHERE id = $1`,
		repoID,
	).Scan(&version))

	return version
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

func TestScannerWorkerRemovalVersioning(t *testing.T) {
	db := newScannerTestDB(t)

	ctx := context.Background()
	repoID := "owner/repository"

	t.Run("removal keeps the last applied version", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		gh := &fakeGithub{err: github.ErrNotFound}
		w, _ := newTestWorker(db.DB, db.Queries, gh, testConfig())

		require.NoError(t, w.DoScanIteration())

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.False(t, active)
		require.Equal(t, int32(1), getRepoActiveVersion(t, ctx, db.DB, repoID))

		// A delayed event carrying the version that was current before removal
		// must not reactivate the repo.
		require.NoError(t, db.Queries.CreateOrUpdateRepo(ctx, dbGen.CreateOrUpdateRepoParams{
			ID:            repoID,
			Owner:         "owner",
			Name:          "repository",
			LatestSeenTag: "v1.0.0",
			Active:        true,
			ActiveVersion: 1,
		}))

		tag, active = getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.False(t, active)
		require.Equal(t, int32(1), getRepoActiveVersion(t, ctx, db.DB, repoID))

		// A genuinely newer subscription event still reactivates it.
		require.NoError(t, db.Queries.CreateOrUpdateRepo(ctx, dbGen.CreateOrUpdateRepoParams{
			ID:            repoID,
			Owner:         "owner",
			Name:          "repository",
			LatestSeenTag: "v2.0.0",
			Active:        true,
			ActiveVersion: 2,
		}))

		tag, active = getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v2.0.0", tag)
		require.True(t, active)
		require.Equal(t, int32(2), getRepoActiveVersion(t, ctx, db.DB, repoID))

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventRepositoryRemoved)
		require.NoError(t, err)
		require.EqualValues(t, 1, eventCount)
	})

	t.Run("removal is skipped when the version changes during the scan", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		calls := 0
		gh := funcGithub(func(context.Context, string, string) (string, error) {
			calls++
			if calls == 1 {
				require.NoError(t, db.Queries.CreateOrUpdateRepo(context.Background(), dbGen.CreateOrUpdateRepoParams{
					ID:            repoID,
					Owner:         "owner",
					Name:          "repository",
					LatestSeenTag: "v1.0.0",
					Active:        true,
					ActiveVersion: 2,
				}))
			}
			return "", github.ErrNotFound
		})
		w, _ := newTestWorker(db.DB, db.Queries, gh, testConfig())

		require.NoError(t, w.DoScanIteration())

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.True(t, active)
		require.Equal(t, int32(2), getRepoActiveVersion(t, ctx, db.DB, repoID))

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventRepositoryRemoved)
		require.NoError(t, err)
		require.Zero(t, eventCount)

		// Once the lease expires, the next scan uses the new version and
		// removes the repo.
		_, err = db.DB.ExecContext(ctx,
			`UPDATE repositories SET leased_until = now() - interval '1 second' WHERE id = $1`,
			repoID,
		)
		require.NoError(t, err)

		require.NoError(t, w.DoScanIteration())

		tag, active = getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v1.0.0", tag)
		require.False(t, active)
		require.Equal(t, int32(2), getRepoActiveVersion(t, ctx, db.DB, repoID))

		eventCount, err = db.Queries.GetEventTypeCount(ctx, rabbitmq.EventRepositoryRemoved)
		require.NoError(t, err)
		require.EqualValues(t, 1, eventCount)
	})
}

func TestScannerWorkerReleaseIdempotency(t *testing.T) {
	db := newScannerTestDB(t)

	ctx := context.Background()
	repoID := "owner/repository"

	leaseRepo := func(t *testing.T) dbGen.Repository {
		t.Helper()

		repos, err := db.Queries.GetReposScanBatch(ctx, dbGen.GetReposScanBatchParams{
			Limit: 10,
			LeasedUntil: sql.NullTime{
				Valid: true,
				Time:  time.Now().Add(time.Minute),
			},
		})
		require.NoError(t, err)
		require.Len(t, repos, 1)

		return repos[0]
	}

	t.Run("stale snapshot does not duplicate release or event", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		// Two scanners can lease the same pre-release snapshot when a lease
		// expires during the GitHub call. Both then try to record the same tag.
		staleRepo := leaseRepo(t)
		w, _ := newTestWorker(db.DB, db.Queries, &fakeGithub{}, testConfig())

		require.NoError(t, w.updateRepo(ctx, staleRepo, "v2.0.0"))
		require.NoError(t, w.updateRepo(ctx, staleRepo, "v2.0.0"))

		var releaseCount int
		require.NoError(t, db.DB.QueryRowContext(ctx, `SELECT count(*) FROM releases`).Scan(&releaseCount))
		require.Equal(t, 1, releaseCount)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventReleaseDetected)
		require.NoError(t, err)
		require.EqualValues(t, 1, eventCount)

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v2.0.0", tag)
		require.True(t, active)
	})

	t.Run("existing release is not re-emitted", func(t *testing.T) {
		resetScannerDB(t, ctx, db.DB)
		insertRepo(t, ctx, db.Queries, repoID, "v1.0.0")

		_, err := db.Queries.CreateRelease(ctx, dbGen.CreateReleaseParams{
			RepositoryID: repoID,
			Tag:          "v2.0.0",
		})
		require.NoError(t, err)

		staleRepo := leaseRepo(t)
		w, _ := newTestWorker(db.DB, db.Queries, &fakeGithub{}, testConfig())

		require.NoError(t, w.updateRepo(ctx, staleRepo, "v2.0.0"))

		var releaseCount int
		require.NoError(t, db.DB.QueryRowContext(ctx, `SELECT count(*) FROM releases`).Scan(&releaseCount))
		require.Equal(t, 1, releaseCount)

		eventCount, err := db.Queries.GetEventTypeCount(ctx, rabbitmq.EventReleaseDetected)
		require.NoError(t, err)
		require.Zero(t, eventCount)

		tag, active := getRepoState(t, ctx, db.DB, repoID)
		require.Equal(t, "v2.0.0", tag)
		require.True(t, active)
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
