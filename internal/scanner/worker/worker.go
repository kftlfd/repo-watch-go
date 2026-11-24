package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"repo-watch/internal/events"
	"repo-watch/internal/github"
	"repo-watch/internal/observability"
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
		iterCtx, cancel := context.WithTimeout(ctx, w.cfg.IterationTimeout)
		correlationID := observability.NewCorrelationID()
		iterCtx = observability.WithCorrelationID(iterCtx, correlationID)
		err := w.DoScanIteration(iterCtx, correlationID)
		cancel()

		if err != nil {
			w.logger.Error("scanner iteration error", "error", err, "correlation_id", correlationID)
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
		callCtx, span := observability.Tracer().Start(ctx, "github get latest release tag",
			trace.WithSpanKind(trace.SpanKindClient),
		)
		tag, err := w.gh.GetLatestReleaseTag(callCtx, repo.Owner, repo.Name)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()

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

func (w *scannerWorker) DoScanIteration(ctx context.Context, correlationID string) (err error) {
	ctx, span := observability.Tracer().Start(ctx, "scan iteration",
		trace.WithNewRoot(),
		trace.WithAttributes(observability.Attr.CorrelationID(correlationID)),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	w.logger.Info("new iteration", "correlation_id", correlationID)

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
		if ctx.Err() != nil {
			break
		}

		stopBatch, err := w.scanRepository(ctx, repo, correlationID)
		if err != nil {
			return err
		}
		if stopBatch {
			break
		}
	}

	return nil
}

// scanRepository scans one repository and persists the result. It reports
// stopBatch when a client-wide failure, such as a rate limit, means the rest
// of the batch should wait for the next iteration.
func (w *scannerWorker) scanRepository(
	ctx context.Context,
	repo dbGen.Repository,
	correlationID string,
) (stopBatch bool, err error) {
	ctx, span := observability.Tracer().Start(ctx, "scan repository")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	latestTag, err := w.fetchLatestTag(ctx, repo)
	if err != nil {
		w.logger.Info("repo scan",
			"repo", repo.ID,
			"status", "gh error",
			"err", err,
			"correlation_id", correlationID,
		)

		if errors.Is(err, github.ErrNotFound) {
			writeCtx, cancel := persistenceContext(ctx)
			removeErr := w.removeRepo(writeCtx, repo)
			cancel()
			if removeErr != nil {
				return false, removeErr
			}
			return false, nil
		}

		// A rate limit applies to the whole client, so stop this batch
		// instead of immediately requesting every remaining repo.
		if errors.Is(err, github.ErrRateLimit) {
			w.logger.Info("stopping batch after github rate limit", "correlation_id", correlationID)
			return true, nil
		}

		return false, nil
	}

	if repo.LatestSeenTag != latestTag {
		w.logger.Info("repo scan",
			"repo", repo.ID,
			"status", "new release",
			"tag", latestTag,
			"correlation_id", correlationID,
		)
	} else {
		w.logger.Info("repo scan",
			"repo", repo.ID,
			"status", "not changed",
			"correlation_id", correlationID,
		)
	}

	writeCtx, cancel := persistenceContext(ctx)
	updateErr := w.updateRepo(writeCtx, repo, latestTag)
	cancel()
	if updateErr != nil {
		return false, fmt.Errorf("update error: %w", updateErr)
	}

	w.met.ScanOk()
	return false, nil
}

func (w *scannerWorker) updateRepo(ctx context.Context, repo dbGen.Repository, latestTag string) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := w.q.WithTx(tx)

	if err := qtx.MarkRepoProcessed(ctx, dbGen.MarkRepoProcessedParams{
		ID:            repo.ID,
		LatestSeenTag: latestTag,
	}); err != nil {
		return fmt.Errorf("mark repo processed: %w", err)
	}

	if repo.LatestSeenTag != latestTag {
		inserted, err := qtx.CreateRelease(ctx, dbGen.CreateReleaseParams{
			RepositoryID: repo.ID,
			Tag:          latestTag,
		})
		if err != nil {
			return fmt.Errorf("create release: %w", err)
		}

		if inserted > 0 {
			payload, err := json.Marshal(
				events.EventReleaseDetectedPayload{
					RepositoryID: repo.ID,
					Owner:        repo.Owner,
					Name:         repo.Name,
					Tag:          latestTag,
				})
			if err != nil {
				return fmt.Errorf("marshal outbox payload json: %w", err)
			}

			traceParent, _ := observability.GetTraceParent(ctx)
			_, err = qtx.CreateOutboxRow(ctx, dbGen.CreateOutboxRowParams{
				EventType:     events.EventReleaseDetected,
				EventVersion:  events.VersionReleaseDetected,
				CorrelationID: observability.GetCorrelationID(ctx),
				TraceParent:   traceParent,
				Payload:       payload,
			})
			if err != nil {
				return fmt.Errorf("insert into outbox: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit repo update: %w", err)
	}

	return nil
}

func (w *scannerWorker) removeRepo(ctx context.Context, repo dbGen.Repository) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := w.q.WithTx(tx)

	affected, err := qtx.MarkRepoInactive(ctx, dbGen.MarkRepoInactiveParams{
		ID:            repo.ID,
		ActiveVersion: repo.ActiveVersion,
	})
	if err != nil {
		return fmt.Errorf("mark repo inactive: %w", err)
	}

	// A newer repository.changed event landed while the scan was in flight.
	// Leave its state alone; the next scan will decide whether to remove it.
	if affected == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit repo removal: %w", err)
		}
		return nil
	}

	payload, err := json.Marshal(
		events.EventRepositoryRemovedPayload{
			RepositoryID: repo.ID,
		})
	if err != nil {
		return fmt.Errorf("marshal outbox payload json: %w", err)
	}

	traceParent, _ := observability.GetTraceParent(ctx)
	_, err = qtx.CreateOutboxRow(ctx, dbGen.CreateOutboxRowParams{
		EventType:     events.EventRepositoryRemoved,
		EventVersion:  events.VersionRepositoryRemoved,
		CorrelationID: observability.GetCorrelationID(ctx),
		TraceParent:   traceParent,
		Payload:       payload,
	})
	if err != nil {
		return fmt.Errorf("insert into outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit repo removal: %w", err)
	}

	return nil
}
