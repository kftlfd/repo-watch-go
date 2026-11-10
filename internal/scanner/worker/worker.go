package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"repo-watch/internal/github"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/scanner/db/dbGen"
)

type GithubClient interface {
	GetLatestReleaseTag(ctx context.Context, owner, repo string) (string, error)
}

const (
	persistenceTimeout = 5 * time.Second

	defaultGithubMaxAttempts = 3
	defaultGithubRetryBase   = 500 * time.Millisecond
	maxGithubRetryDelay      = 5 * time.Second
)

type Config struct {
	BatchSize        int32
	Interval         time.Duration
	IterationTimeout time.Duration
	LeaseDuration    time.Duration

	GithubMaxAttempts int
	GithubRetryBase   time.Duration
}

type Deps struct {
	Cfg          Config
	Logger       *slog.Logger
	DB           *sql.DB
	Queries      *dbGen.Queries
	Publisher    rabbitmq.RabbitJSONPublisher
	GithubClient GithubClient
	Metrics      Metrics
}

type scannerWorker struct {
	cfg    Config
	logger *slog.Logger
	db     *sql.DB
	q      *dbGen.Queries
	pub    rabbitmq.RabbitJSONPublisher
	gh     GithubClient
	met    Metrics
}

func NewScannerWorker(d Deps) *scannerWorker {
	return &scannerWorker{
		cfg:    d.Cfg,
		logger: d.Logger.With("module", "scanner_worker"),
		db:     d.DB,
		q:      d.Queries,
		pub:    d.Publisher,
		gh:     d.GithubClient,
		met:    d.Metrics,
	}
}

func (w *scannerWorker) Start(ctx context.Context) error {
	w.logger.Info("starting")

	for {
		if err := w.DoScanIteration(); err != nil {
			w.logger.Error("scanner iteration error", "error", err)
			w.met.ScanErr()
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(w.cfg.Interval):
		}
	}
}

// persistenceContext detaches from the iteration deadline. The GitHub call
// must respect the iteration budget, but once it succeeds the result should
// still be stored even if the iteration deadline expires while the request is
// finishing (or shortly after). The independent timeout keeps shutdown bounded.
func persistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
}

// fetchLatestTag retries transient GitHub failures and rate limits that come
// with a usable retry hint. Rate limits without a hint are not retried inline:
// the repo stays leased and is picked up again after the lease expires.
func (w *scannerWorker) fetchLatestTag(ctx context.Context, repo dbGen.Repository) (string, error) {
	attempts := w.cfg.GithubMaxAttempts
	if attempts < 1 {
		attempts = defaultGithubMaxAttempts
	}

	backoff := w.cfg.GithubRetryBase
	if backoff <= 0 {
		backoff = defaultGithubRetryBase
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		tag, err := w.gh.GetLatestReleaseTag(ctx, repo.Owner, repo.Name)
		if err == nil {
			return tag, nil
		}
		lastErr = err

		wait, retry := githubRetryDelay(err, backoff)
		if !retry || attempt == attempts {
			break
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= wait {
			break
		}
		if !sleepContext(ctx, wait) {
			break
		}

		backoff *= 2
		if backoff > maxGithubRetryDelay {
			backoff = maxGithubRetryDelay
		}
	}

	return "", lastErr
}

func githubRetryDelay(err error, backoff time.Duration) (time.Duration, bool) {
	var rateErr *github.RateLimitError
	if errors.As(err, &rateErr) {
		// Without a retry hint, retrying inline would be a tight loop.
		if rateErr.RetryAfter <= 0 {
			return 0, false
		}
		return rateErr.RetryAfter, true
	}

	if errors.Is(err, github.ErrTransientError) {
		return backoff, true
	}

	return 0, false
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *scannerWorker) DoScanIteration() error {
	w.logger.Info("new iteration")

	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.IterationTimeout)
	defer cancel()

	batch, err := w.q.GetReposScanBatch(ctx, dbGen.GetReposScanBatchParams{
		Limit: w.cfg.BatchSize,
		LeasedUntil: sql.NullTime{
			Valid: true,
			Time:  time.Now().Add(w.cfg.LeaseDuration),
		},
	})
	if err != nil {
		return fmt.Errorf("get batch: %w", err)
	}

	for _, repo := range batch {
		latestTag, err := w.fetchLatestTag(ctx, repo)
		if err != nil {
			w.logger.Info("repo scan", "repo", repo.ID, "status", "gh error", "err", err)

			if errors.Is(err, github.ErrNotFound) {
				writeCtx, cancel := persistenceContext(ctx)
				err := w.removeRepo(writeCtx, repo.ID)
				cancel()
				if err != nil {
					return err
				}
				continue
			}

			// A rate limit applies to the whole client, so stop this batch
			// instead of immediately requesting every remaining repo.
			if errors.Is(err, github.ErrRateLimit) {
				w.logger.Info("stopping batch after github rate limit")
				break
			}

			continue
		}

		if repo.LatestSeenTag != latestTag {
			w.logger.Info("repo scan", "repo", repo.ID, "status", "new release", "tag", latestTag)
		} else {
			w.logger.Info("repo scan", "repo", repo.ID, "status", "not changed")
		}

		writeCtx, cancel := persistenceContext(ctx)
		err = w.updateRepo(writeCtx, repo, latestTag)
		cancel()
		if err != nil {
			return fmt.Errorf("update error: %w", err)
		}

		w.met.ScanOk()
	}

	return nil
}

func (w *scannerWorker) updateRepo(ctx context.Context, repo dbGen.Repository, latestTag string) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	qtx := w.q.WithTx(tx)

	if err := qtx.MarkRepoProcessed(ctx, dbGen.MarkRepoProcessedParams{
		ID:            repo.ID,
		LatestSeenTag: latestTag,
	}); err != nil {
		return fmt.Errorf("mark repo processed: %w", err)
	}

	if repo.LatestSeenTag != latestTag {
		_, err := qtx.CreateRelease(ctx, dbGen.CreateReleaseParams{
			RepositoryID: repo.ID,
			Tag:          latestTag,
		})
		if err != nil {
			return fmt.Errorf("create release: %w", err)
		}

		payload, err := json.Marshal(
			rabbitmq.EventReleaseDetectedPayload{
				RepositoryID: repo.ID,
				Owner:        repo.Owner,
				Name:         repo.Name,
				Tag:          latestTag,
			})
		if err != nil {
			return fmt.Errorf("marshal outbox payload json: %w", err)
		}

		_, err = qtx.CreateOutboxRow(ctx, dbGen.CreateOutboxRowParams{
			EventType: rabbitmq.EventReleaseDetected,
			Payload:   payload,
		})
		if err != nil {
			return fmt.Errorf("insert into outbox: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit repo update: %w", err)
	}

	return nil
}

func (w *scannerWorker) removeRepo(ctx context.Context, repoID string) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	qtx := w.q.WithTx(tx)

	payload, err := json.Marshal(
		rabbitmq.EventRepositoryRemovedPayload{
			RepositoryID: repoID,
		})
	if err != nil {
		return fmt.Errorf("marshal outbox payload json: %w", err)
	}

	if err := qtx.MarkRepoInactive(ctx, repoID); err != nil {
		return fmt.Errorf("mark repo inactive: %w", err)
	}

	_, err = qtx.CreateOutboxRow(ctx, dbGen.CreateOutboxRowParams{
		EventType: rabbitmq.EventRepositoryRemoved,
		Payload:   payload,
	})
	if err != nil {
		return fmt.Errorf("insert into outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit repo removal: %w", err)
	}

	return nil
}
