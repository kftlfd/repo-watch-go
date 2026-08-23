package subs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"repo-watch/internal/subscription/database"
	"repo-watch/pkg/github"
	"time"
)

var (
	ErrRepoNotFound         = errors.New("repo not found")
	ErrSubscriptionConflict = errors.New("active subscription already exist")
)

type SubsService interface {
	ConfirmSubscription(ctx context.Context, token string) error
	CreateSubscription(ctx context.Context, email, repo string) error
	DeactivateSubscription(ctx context.Context, token string) error
	GetSubscriptionsList(ctx context.Context, email string) ([]string, error)
}

type Deps struct {
	GithubClient github.Client
	DB           *sql.DB
	Queries      *database.Queries
}

type service struct {
	gh github.Client
	db *sql.DB
	q  *database.Queries
}

func NewService(d Deps) *service {
	return &service{
		gh: d.GithubClient,
		db: d.DB,
		q:  d.Queries,
	}
}

func (s *service) CreateSubscription(ctx context.Context, email, repo string) error {
	emailInfo, err := verifyEmailInfo(email)
	if err != nil {
		return err
	}

	repoInfo, err := verifyRepoInfo(repo)
	if err != nil {
		return err
	}

	tag, err := s.gh.GetLatestReleaseTag(ctx, repoInfo.owner, repoInfo.name)
	if err != nil {
		return ErrRepoNotFound
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	qtx := s.q.WithTx(tx)

	exists, err := qtx.ActiveSubscriptionExists(ctx, database.ActiveSubscriptionExistsParams{
		Email:        emailInfo.email,
		RepositoryID: repoInfo.repoId,
	})
	if err != nil {
		return err
	}
	if exists {
		return ErrSubscriptionConflict
	}

	_, err = qtx.CreateRepository(ctx, database.CreateRepositoryParams{
		ID:      repoInfo.repoId,
		GhOwner: repoInfo.owner,
		GhName:  repoInfo.name,
		InitTag: tag,
	})
	if err != nil {
		return err
	}

	sub, err := qtx.CreateSubscription(ctx, database.CreateSubscriptionParams{
		Email:        emailInfo.email,
		RepositoryID: repoInfo.repoId,
	})
	if err != nil {
		return err
	}

	tokenVal, err := newToken()
	if err != nil {
		return err
	}

	_, err = qtx.CreateToken(ctx, database.CreateTokenParams{
		SubscriptionID: sub.ID,
		TokenType:      TokenTypeConfirm,
		TokenVal:       tokenVal,
	})
	if err != nil {
		return err
	}

	_, err = qtx.CreateOutboxRow(ctx, database.CreateOutboxRowParams{
		EventType: "email.send",
		Payload:   []byte(`{"type":"send-confirmation-email","email":"email","token":"token"}`),
	})
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *service) ConfirmSubscription(ctx context.Context, token string) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	qtx := s.q.WithTx(tx)

	t, err := qtx.FindTokenWDate(ctx, database.FindTokenWDateParams{
		TokenType: TokenTypeConfirm,
		TokenVal:  token,
		CreatedAt: time.Now().Add(-time.Hour),
	})
	if err != nil {
		return err
	}

	err = qtx.DeleteTokenById(ctx, t.ID)
	if err != nil {
		return err
	}

	sub, err := qtx.GetSubscriptionByID(ctx, t.SubscriptionID)
	if err != nil {
		return err
	}

	repo, err := qtx.GetRepositoryByID(ctx, sub.RepositoryID)
	if err != nil {
		return err
	}

	unsubTokenVal, err := newToken()
	if err != nil {
		return err
	}

	unsubToken, err := qtx.CreateToken(ctx, database.CreateTokenParams{
		SubscriptionID: sub.ID,
		TokenType:      TokenTypeUnsub,
		TokenVal:       unsubTokenVal,
	})
	if err != nil {
		return err
	}

	updSub, err := qtx.ActivateSubscription(ctx, sub.ID)
	if err != nil {
		return err
	}

	updRepo, err := qtx.IncrementRepositoryActiveCount(ctx, repo.ID)
	if err != nil {
		return err
	}

	_, err = qtx.CreateOutboxRow(ctx, database.CreateOutboxRowParams{
		EventType: "subscription.changed",
		Payload:   []byte(`"subscription changed event"` + updSub.Email + unsubToken.TokenVal),
	})
	if err != nil {
		return err
	}

	if updRepo.ActiveVersion != repo.ActiveVersion {
		_, err = qtx.CreateOutboxRow(ctx, database.CreateOutboxRowParams{
			EventType: "repository.changed",
			Payload:   []byte(`"repository active state changed"`),
		})
		if err != nil {
			return err
		}
	}

	_, err = qtx.CreateOutboxRow(ctx, database.CreateOutboxRowParams{
		EventType: "email.send",
		Payload:   []byte(`"subscription confirmed email"`),
	})

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *service) DeactivateSubscription(ctx context.Context, token string) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	qtx := s.q.WithTx(tx)

	t, err := qtx.FindToken(ctx, database.FindTokenParams{
		TokenType: TokenTypeUnsub,
		TokenVal:  token,
	})
	if err != nil {
		return err
	}

	err = qtx.DeleteTokenById(ctx, t.ID)
	if err != nil {
		return err
	}

	sub, err := qtx.GetSubscriptionByID(ctx, t.SubscriptionID)
	if err != nil {
		return err
	}

	repo, err := qtx.GetRepositoryByID(ctx, sub.RepositoryID)
	if err != nil {
		return err
	}

	updSub, err := qtx.DeactivateSubscription(ctx, sub.ID)
	if err != nil {
		return err
	}

	updRepo, err := qtx.DecrementRepositoryActiveCount(ctx, repo.ID)
	if err != nil {
		return err
	}

	_, err = qtx.CreateOutboxRow(ctx, database.CreateOutboxRowParams{
		EventType: "subscription.changed",
		Payload:   []byte(`"subscription changed event"` + updSub.Email),
	})
	if err != nil {
		return err
	}

	if updRepo.ActiveVersion != repo.ActiveVersion {
		_, err = qtx.CreateOutboxRow(ctx, database.CreateOutboxRowParams{
			EventType: "repository.changed",
			Payload:   []byte(`"repository active state changed"`),
		})
		if err != nil {
			return err
		}
	}

	_, err = qtx.CreateOutboxRow(ctx, database.CreateOutboxRowParams{
		EventType: "email.send",
		Payload:   []byte(`"unsubscribed email"`),
	})

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *service) GetSubscriptionsList(ctx context.Context, email string) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	qtx := s.q.WithTx(tx)

	subs, err := qtx.ListSubscriptionsByEmail(ctx, email)
	if err != nil {
		return nil, err
	}

	return subs, nil
}
