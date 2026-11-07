package subs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"repo-watch/internal/emails"
	"repo-watch/internal/github"
	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/database"
)

var (
	ErrInvalidInput         = errors.New("invalid input")
	ErrRepoNotFound         = errors.New("repo not found")
	ErrRateLimit            = errors.New("rate limit")
	ErrUpstreamError        = errors.New("upstream error")
	ErrSubscriptionConflict = errors.New("active subscription already exist")
)

type GithubClient interface {
	GetLatestReleaseTag(ctx context.Context, owner, repo string) (string, error)
}

type Deps struct {
	GithubClient GithubClient
	DB           *sql.DB
	Queries      *database.Queries
}

type service struct {
	gh GithubClient
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
		return ErrInvalidInput
	}

	repoInfo, err := verifyRepoInfo(repo)
	if err != nil {
		return ErrInvalidInput
	}

	tag, err := s.gh.GetLatestReleaseTag(ctx, repoInfo.owner, repoInfo.name)
	if err != nil {
		if errors.Is(err, github.ErrNotFound) {
			return ErrRepoNotFound
		}
		if errors.Is(err, github.ErrRateLimit) {
			return ErrRateLimit
		}
		return ErrUpstreamError
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	qtx := s.q.WithTx(tx)

	_, err = qtx.CreateRepository(ctx, database.CreateRepositoryParams{
		ID:      repoInfo.repoId,
		Owner:   repoInfo.owner,
		Name:    repoInfo.name,
		InitTag: tag,
	})
	if errors.Is(err, sql.ErrNoRows) {
		_, err = qtx.GetRepositoryByID(ctx, repoInfo.repoId)
	}
	if err != nil {
		return err
	}

	dbSub, err := qtx.CreateSubscription(ctx, database.CreateSubscriptionParams{
		Email:        emailInfo.email,
		RepositoryID: repoInfo.repoId,
	})
	if errors.Is(err, sql.ErrNoRows) {
		dbSub, err = qtx.GetSubscription(ctx, database.GetSubscriptionParams{
			Email:        emailInfo.email,
			RepositoryID: repoInfo.repoId,
		})
	}
	if err != nil {
		return err
	}
	if dbSub.Active {
		return ErrSubscriptionConflict
	}

	tokenVal, err := newToken()
	if err != nil {
		return err
	}

	_, err = qtx.CreateToken(ctx, database.CreateTokenParams{
		SubscriptionID: dbSub.ID,
		TokenType:      TokenTypeConfirm,
		TokenVal:       tokenVal,
	})
	if err != nil {
		return err
	}

	emailData, err := json.Marshal(emails.EmailConfirmSubscriptionData{
		RepoID:       repoInfo.repoId,
		ConfirmToken: tokenVal,
	})
	if err != nil {
		return err
	}

	if err = saveToOutbox(ctx, qtx,
		rabbitmq.EventEmailQueued,
		rabbitmq.EventEmailQueuedPayload{
			Recipient: emailInfo.email,
			Type:      emails.EmailConfirmSubscription,
			Data:      emailData,
		},
	); err != nil {
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
		CreatedAt: time.Now().Add(-ConfirmTokenValidity),
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

	if err = saveToOutbox(ctx, qtx,
		rabbitmq.EventSubscriptionChanged,
		rabbitmq.EventSubscriptionChangedPayload{
			SubscriptionID: updSub.ID,
			RepositoryID:   updRepo.ID,
			Email:          updSub.Email,
			Active:         updSub.Active,
			Version:        updSub.ActiveVersion,
			UnsubToken:     unsubToken.TokenVal,
			UpdatedAt:      updSub.UpdatedAt,
		},
	); err != nil {
		return err
	}

	if updRepo.ActiveVersion != repo.ActiveVersion {
		if err = saveToOutbox(ctx, qtx,
			rabbitmq.EventRepositoryChanged,
			rabbitmq.EventRepositoryChangedPayload{
				RepositoryID:  updRepo.ID,
				Owner:         updRepo.Owner,
				Name:          updRepo.Name,
				LatestSeenTag: updRepo.InitTag,
				Active:        updRepo.ActiveSubscriptionsCount > 0,
				ActiveVersion: updRepo.ActiveVersion,
				UpdatedAt:     updRepo.UpdatedAt,
			},
		); err != nil {
			return err
		}
	}

	emailData, err := json.Marshal(emails.EmailSubscribedData{
		RepoID: updRepo.ID,
	})
	if err != nil {
		return err
	}

	if err = saveToOutbox(ctx, qtx,
		rabbitmq.EventEmailQueued,
		rabbitmq.EventEmailQueuedPayload{
			Recipient: updSub.Email,
			Type:      emails.EmailSubscribed,
			Data:      emailData,
		},
	); err != nil {
		return err
	}

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

	if err = saveToOutbox(ctx, qtx,
		rabbitmq.EventSubscriptionChanged,
		rabbitmq.EventSubscriptionChangedPayload{
			SubscriptionID: updSub.ID,
			RepositoryID:   updRepo.ID,
			Email:          updSub.Email,
			Active:         updSub.Active,
			Version:        updSub.ActiveVersion,
			UpdatedAt:      updSub.UpdatedAt,
		},
	); err != nil {
		return err
	}

	if updRepo.ActiveVersion != repo.ActiveVersion {
		if err = saveToOutbox(ctx, qtx,
			rabbitmq.EventRepositoryChanged,
			rabbitmq.EventRepositoryChangedPayload{
				RepositoryID:  updRepo.ID,
				Owner:         updRepo.Owner,
				Name:          updRepo.Name,
				LatestSeenTag: updRepo.InitTag,
				Active:        updRepo.ActiveSubscriptionsCount > 0,
				ActiveVersion: updRepo.ActiveVersion,
				UpdatedAt:     updRepo.UpdatedAt,
			},
		); err != nil {
			return err
		}
	}

	emailData, err := json.Marshal(emails.EmailUnsubscribedData{
		RepoID: updRepo.ID,
	})
	if err != nil {
		return err
	}

	if err = saveToOutbox(ctx, qtx,
		rabbitmq.EventEmailQueued,
		rabbitmq.EventEmailQueuedPayload{
			Recipient: updSub.Email,
			Type:      emails.EmailUnsubscribed,
			Data:      emailData,
		},
	); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *service) GetSubscriptionsList(ctx context.Context, email string) ([]string, error) {
	subs, err := s.q.ListSubscriptionsByEmail(ctx, email)
	if err != nil {
		return nil, err
	}

	return subs, nil
}

func saveToOutbox(ctx context.Context, qtx *database.Queries, eventType string, eventPayload any) error {
	jsonData, err := json.Marshal(eventPayload)
	if err != nil {
		return fmt.Errorf("json marshal event: %w", err)
	}

	_, err = qtx.CreateOutboxRow(ctx, database.CreateOutboxRowParams{
		EventType: eventType,
		Payload:   jsonData,
	})

	return err
}
