package subs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"repo-watch/internal/emails"
	"repo-watch/internal/events"
	"repo-watch/internal/github"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/identity"
	"repo-watch/internal/subscription/outbox"
)

var (
	ErrInvalidInput                = errors.New("invalid input")
	ErrRepoNotFound                = errors.New("repo not found")
	ErrRateLimit                   = errors.New("rate limit")
	ErrUpstreamError               = errors.New("upstream error")
	ErrSubscriptionConflict        = errors.New("active subscription already exist")
	ErrSubscriptionAlreadyActive   = errors.New("subscription already active")
	ErrSubscriptionAlreadyInactive = errors.New("subscription already inactive")
)

type GithubClient interface {
	GetLatestReleaseTag(ctx context.Context, owner, repo string) (string, error)
}

type Deps struct {
	GithubClient GithubClient
	DB           *sql.DB
	Queries      *dbGen.Queries
}

type service struct {
	gh GithubClient
	db *sql.DB
	q  *dbGen.Queries
}

func NewService(d Deps) *service {
	return &service{
		gh: d.GithubClient,
		db: d.DB,
		q:  d.Queries,
	}
}

// CreateSubscription creates a subscription for repo. Anonymous callers get a
// confirmation email; verified callers are activated immediately.
func (s *service) CreateSubscription(ctx context.Context, email identity.Email, repo string, verified bool) error {
	repoInfo, err := verifyRepoInfo(repo)
	if err != nil {
		return ErrInvalidInput
	}

	tag := ""
	existingRepo, err := s.q.GetRepositoryByID(ctx, repoInfo.repoId)
	switch {
	case err == nil:
		tag = existingRepo.InitTag
	case errors.Is(err, sql.ErrNoRows):
		tag, err = s.gh.GetLatestReleaseTag(ctx, repoInfo.owner, repoInfo.name)
		if err != nil {
			if errors.Is(err, github.ErrNotFound) {
				return ErrRepoNotFound
			}
			if errors.Is(err, github.ErrRateLimit) {
				return ErrRateLimit
			}
			return ErrUpstreamError
		}
	default:
		return fmt.Errorf("lookup repository: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	_, err = qtx.CreateRepository(ctx, dbGen.CreateRepositoryParams{
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

	dbSub, err := qtx.CreateSubscription(ctx, dbGen.CreateSubscriptionParams{
		Email:        email.String(),
		RepositoryID: repoInfo.repoId,
	})
	if errors.Is(err, sql.ErrNoRows) {
		dbSub, err = qtx.GetSubscription(ctx, dbGen.GetSubscriptionParams{
			Email:        email.String(),
			RepositoryID: repoInfo.repoId,
		})
	}
	if err != nil {
		return err
	}

	if dbSub.Active {
		if verified {
			return ErrSubscriptionConflict
		}
		return nil
	}

	if verified {
		if err = s.activateSubscription(ctx, qtx, dbSub); err != nil {
			if errors.Is(err, ErrSubscriptionAlreadyActive) {
				if commitErr := tx.Commit(); commitErr != nil {
					return commitErr
				}
				return ErrSubscriptionConflict
			}
			return err
		}

		if err = tx.Commit(); err != nil {
			return err
		}

		return nil
	}

	tokenVal, err := newToken()
	if err != nil {
		return err
	}

	_, err = qtx.UpsertSubscriptionToken(ctx, dbGen.UpsertSubscriptionTokenParams{
		SubscriptionID: dbSub.ID,
		TokenType:      TokenTypeConfirm,
		TokenHash:      hashToken(tokenVal),
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

	if err = outbox.NewEmail(ctx, qtx, events.EventEmailQueuedPayload{
		Recipient: email.String(),
		Type:      emails.EmailConfirmSubscription,
		Data:      emailData,
	}); err != nil {
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
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	tokenParams := dbGen.FindSubscriptionTokenWDateParams{
		TokenType: TokenTypeConfirm,
		TokenHash: hashToken(token),
		CreatedAt: time.Now().Add(-SubscriptionTokenConfirmTTL),
	}
	candidate, err := qtx.FindSubscriptionTokenWDate(ctx, tokenParams)
	if err != nil {
		return err
	}

	sub, err := qtx.GetSubscriptionByIDForUpdate(ctx, candidate.SubscriptionID)
	if err != nil {
		return err
	}

	t, err := qtx.FindSubscriptionTokenWDateForUpdate(ctx, dbGen.FindSubscriptionTokenWDateForUpdateParams{
		TokenType: tokenParams.TokenType,
		TokenHash: tokenParams.TokenHash,
		CreatedAt: tokenParams.CreatedAt,
	})
	if err != nil {
		return err
	}
	if t.SubscriptionID != sub.ID {
		return sql.ErrNoRows
	}

	if err = s.activateSubscription(ctx, qtx, sub); err != nil {
		if errors.Is(err, ErrSubscriptionAlreadyActive) {
			if commitErr := tx.Commit(); commitErr != nil {
				return commitErr
			}
		}
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
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	tokenParams := dbGen.FindSubscriptionTokenParams{
		TokenType: TokenTypeUnsub,
		TokenHash: hashToken(token),
	}
	candidate, err := qtx.FindSubscriptionToken(ctx, tokenParams)
	if err != nil {
		return err
	}

	sub, err := qtx.GetSubscriptionByIDForUpdate(ctx, candidate.SubscriptionID)
	if err != nil {
		return err
	}

	t, err := qtx.FindSubscriptionTokenForUpdate(ctx, dbGen.FindSubscriptionTokenForUpdateParams{
		TokenType: tokenParams.TokenType,
		TokenHash: tokenParams.TokenHash,
	})
	if err != nil {
		return err
	}
	if t.SubscriptionID != sub.ID {
		return sql.ErrNoRows
	}

	if err = s.deactivateSubscription(ctx, qtx, sub); err != nil {
		if errors.Is(err, ErrSubscriptionAlreadyInactive) {
			if commitErr := tx.Commit(); commitErr != nil {
				return commitErr
			}
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

// DeactivateSubscriptionByRepo deactivates the subscription owned by email for
// repo.
func (s *service) DeactivateSubscriptionByRepo(ctx context.Context, email identity.Email, repo string) error {
	repoInfo, err := verifyRepoInfo(repo)
	if err != nil {
		return ErrInvalidInput
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	sub, err := qtx.GetSubscription(ctx, dbGen.GetSubscriptionParams{
		Email:        email.String(),
		RepositoryID: repoInfo.repoId,
	})
	if err != nil {
		return err
	}

	if err = s.deactivateSubscription(ctx, qtx, sub); err != nil {
		if errors.Is(err, ErrSubscriptionAlreadyInactive) {
			if commitErr := tx.Commit(); commitErr != nil {
				return commitErr
			}
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *service) GetSubscriptionsList(ctx context.Context, email identity.Email) ([]string, error) {
	subs, err := s.q.ListSubscriptionsByEmail(ctx, email.String())
	if err != nil {
		return nil, err
	}

	return subs, nil
}

func (s *service) activateSubscription(ctx context.Context, qtx *dbGen.Queries, sub dbGen.Subscription) error {
	updSub, err := qtx.ActivateSubscription(ctx, sub.ID)
	if errors.Is(err, sql.ErrNoRows) {
		if err = qtx.DeleteSubscriptionTokensBySubIDAndType(ctx, dbGen.DeleteSubscriptionTokensBySubIDAndTypeParams{
			SubscriptionID: sub.ID,
			TokenType:      TokenTypeConfirm,
		}); err != nil {
			return err
		}
		return ErrSubscriptionAlreadyActive
	}
	if err != nil {
		return err
	}

	if err = qtx.DeleteSubscriptionTokensBySubID(ctx, sub.ID); err != nil {
		return err
	}

	unsubTokenVal, err := newToken()
	if err != nil {
		return err
	}

	_, err = qtx.UpsertSubscriptionToken(ctx, dbGen.UpsertSubscriptionTokenParams{
		SubscriptionID: sub.ID,
		TokenType:      TokenTypeUnsub,
		TokenHash:      hashToken(unsubTokenVal),
	})
	if err != nil {
		return err
	}

	updRepo, err := qtx.IncrementRepositoryActiveCount(ctx, sub.RepositoryID)
	if err != nil {
		return err
	}

	if err = outbox.NewSubscriptionChanged(ctx, qtx, events.EventSubscriptionChangedPayload{
		SubscriptionID: updSub.ID,
		RepositoryID:   updRepo.ID,
		Email:          updSub.Email,
		Active:         updSub.Active,
		Version:        updSub.ActiveVersion,
		UnsubToken:     unsubTokenVal,
		UpdatedAt:      updSub.UpdatedAt,
	}); err != nil {
		return err
	}

	if updRepo.ActiveSubscriptionsCount == 1 {
		if err = outbox.NewRepositoryChanged(ctx, qtx, events.EventRepositoryChangedPayload{
			RepositoryID:  updRepo.ID,
			Owner:         updRepo.Owner,
			Name:          updRepo.Name,
			LatestSeenTag: updRepo.InitTag,
			Active:        updRepo.ActiveSubscriptionsCount > 0,
			ActiveVersion: updRepo.ActiveVersion,
			UpdatedAt:     updRepo.UpdatedAt,
		}); err != nil {
			return err
		}
	}

	emailData, err := json.Marshal(emails.EmailSubscribedData{
		RepoID: updRepo.ID,
	})
	if err != nil {
		return err
	}

	return outbox.NewEmail(ctx, qtx, events.EventEmailQueuedPayload{
		Recipient: updSub.Email,
		Type:      emails.EmailSubscribed,
		Data:      emailData,
	})
}

func (s *service) deactivateSubscription(ctx context.Context, qtx *dbGen.Queries, sub dbGen.Subscription) error {
	updSub, err := qtx.DeactivateSubscription(ctx, sub.ID)
	if errors.Is(err, sql.ErrNoRows) {
		if err = qtx.DeleteSubscriptionTokensBySubIDAndType(ctx, dbGen.DeleteSubscriptionTokensBySubIDAndTypeParams{
			SubscriptionID: sub.ID,
			TokenType:      TokenTypeUnsub,
		}); err != nil {
			return err
		}
		return ErrSubscriptionAlreadyInactive
	}
	if err != nil {
		return err
	}

	if err = qtx.DeleteSubscriptionTokensBySubID(ctx, sub.ID); err != nil {
		return err
	}

	updRepo, err := qtx.DecrementRepositoryActiveCount(ctx, sub.RepositoryID)
	if err != nil {
		return err
	}

	if err = outbox.NewSubscriptionChanged(ctx, qtx, events.EventSubscriptionChangedPayload{
		SubscriptionID: updSub.ID,
		RepositoryID:   updRepo.ID,
		Email:          updSub.Email,
		Active:         updSub.Active,
		Version:        updSub.ActiveVersion,
		UpdatedAt:      updSub.UpdatedAt,
	}); err != nil {
		return err
	}

	if updRepo.ActiveSubscriptionsCount == 0 {
		if err = outbox.NewRepositoryChanged(ctx, qtx, events.EventRepositoryChangedPayload{
			RepositoryID:  updRepo.ID,
			Owner:         updRepo.Owner,
			Name:          updRepo.Name,
			LatestSeenTag: updRepo.InitTag,
			Active:        updRepo.ActiveSubscriptionsCount > 0,
			ActiveVersion: updRepo.ActiveVersion,
			UpdatedAt:     updRepo.UpdatedAt,
		}); err != nil {
			return err
		}
	}

	emailData, err := json.Marshal(emails.EmailUnsubscribedData{
		RepoID: updRepo.ID,
	})
	if err != nil {
		return err
	}

	return outbox.NewEmail(ctx, qtx, events.EventEmailQueuedPayload{
		Recipient: updSub.Email,
		Type:      emails.EmailUnsubscribed,
		Data:      emailData,
	})
}
