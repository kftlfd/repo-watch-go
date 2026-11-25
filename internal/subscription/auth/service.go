package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"repo-watch/internal/securetoken"
	"repo-watch/internal/subscription/db/dbGen"
)

const (
	// DefaultIssuer and DefaultAudience identify tokens issued by this service.
	DefaultIssuer   = "repo-watch"
	DefaultAudience = "repo-watch-api"

	// DefaultAccessTTL is the lifetime of an access token.
	DefaultAccessTTL = 10 * time.Minute

	// DefaultRefreshTTL is the sliding lifetime of a refresh token and session.
	DefaultRefreshTTL = 30 * 24 * time.Hour

	defaultJWTLeeway = 30 * time.Second
)

var ErrTokenReused = errors.New("refresh token reused")

// Config controls token lifetimes and JWT validation.
type Config struct {
	JWTSecret  []byte
	Issuer     string
	Audience   string
	AccessTTL  time.Duration
	RefreshTTL time.Duration
	Leeway     time.Duration
	Now        func() time.Time
}

// Deps are the dependencies of the auth service.
type Deps struct {
	DB      *sql.DB
	Queries *dbGen.Queries
	Config  Config
}

// Service creates sessions, issues token pairs, rotates refresh tokens, and
// validates access tokens against the current session row.
type Service struct {
	db         *sql.DB
	q          *dbGen.Queries
	jwt        *JWTManager
	accessTTL  time.Duration
	refreshTTL time.Duration
	now        func() time.Time
}

// TokenPair is a newly issued access/refresh pair.
type TokenPair struct {
	UserEmail        string
	AccessToken      string
	RefreshToken     string
	AccessExpiresIn  time.Duration
	RefreshExpiresIn time.Duration
}

// AuthenticatedSession is the result of an access-token check.
type AuthenticatedSession struct {
	Session dbGen.Session
	User    dbGen.User
}

// NewService returns an auth service. The JWT secret must be at least 32 bytes.
func NewService(d Deps) (*Service, error) {
	cfg := d.Config
	if cfg.Issuer == "" {
		cfg.Issuer = DefaultIssuer
	}
	if cfg.Audience == "" {
		cfg.Audience = DefaultAudience
	}
	if cfg.AccessTTL <= 0 {
		cfg.AccessTTL = DefaultAccessTTL
	}
	if cfg.RefreshTTL <= 0 {
		cfg.RefreshTTL = DefaultRefreshTTL
	}
	if cfg.Leeway < 0 {
		cfg.Leeway = 0
	}
	if cfg.Leeway == 0 {
		cfg.Leeway = defaultJWTLeeway
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	jwt, err := NewJWTManager(cfg.JWTSecret, cfg.Issuer, cfg.Audience, cfg.Leeway)
	if err != nil {
		return nil, err
	}

	return &Service{
		db:         d.DB,
		q:          d.Queries,
		jwt:        jwt,
		accessTTL:  cfg.AccessTTL,
		refreshTTL: cfg.RefreshTTL,
		now:        cfg.Now,
	}, nil
}

// CreateSession creates a session and refresh token for an existing user and
// returns a new token pair.
func (s *Service) CreateSession(ctx context.Context, email string) (TokenPair, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return TokenPair{}, fmt.Errorf("%w: empty email", ErrInvalidToken)
	}

	now := s.now()
	expiresAt := now.Add(s.refreshTTL)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return TokenPair{}, fmt.Errorf("begin session tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	session, err := qtx.CreateSession(ctx, dbGen.CreateSessionParams{
		UserEmail: email,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return TokenPair{}, err
	}

	pair, err := s.issuePair(ctx, qtx, session, now)
	if err != nil {
		return TokenPair{}, err
	}

	if err = tx.Commit(); err != nil {
		return TokenPair{}, fmt.Errorf("commit session tx: %w", err)
	}

	return pair, nil
}

// RefreshSession rotates a refresh token and returns a new pair. Reusing an
// already-used refresh token revokes the whole session and returns
// ErrTokenReused.
func (s *Service) RefreshSession(ctx context.Context, rawRefreshToken string) (TokenPair, error) {
	if !securetoken.Validate(rawRefreshToken, refreshTokenBytes) {
		return TokenPair{}, ErrInvalidToken
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return TokenPair{}, fmt.Errorf("begin refresh tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	refreshToken, err := qtx.GetRefreshTokenByHashForUpdate(ctx, securetoken.Hash(rawRefreshToken))
	if errors.Is(err, sql.ErrNoRows) {
		return TokenPair{}, ErrInvalidToken
	}
	if err != nil {
		return TokenPair{}, err
	}

	now := s.now()

	if refreshToken.UsedAt.Valid {
		if err := s.revokeSession(ctx, qtx, refreshToken.SessionID); err != nil {
			return TokenPair{}, err
		}
		if err = tx.Commit(); err != nil {
			return TokenPair{}, fmt.Errorf("commit reused refresh tx: %w", err)
		}
		return TokenPair{}, ErrTokenReused
	}

	if refreshToken.RevokedAt.Valid || !now.Before(refreshToken.ExpiresAt) {
		return TokenPair{}, ErrInvalidToken
	}

	session, err := qtx.GetSessionByIDForUpdate(ctx, refreshToken.SessionID)
	if errors.Is(err, sql.ErrNoRows) {
		return TokenPair{}, ErrInvalidToken
	}
	if err != nil {
		return TokenPair{}, err
	}
	if session.RevokedAt.Valid || !now.Before(session.ExpiresAt) {
		return TokenPair{}, ErrInvalidToken
	}

	if _, err = qtx.MarkRefreshTokenUsed(ctx, refreshToken.ID); errors.Is(err, sql.ErrNoRows) {
		return TokenPair{}, ErrTokenReused
	} else if err != nil {
		return TokenPair{}, err
	}

	if _, err = qtx.TouchSession(ctx, dbGen.TouchSessionParams{
		ID:        session.ID,
		ExpiresAt: now.Add(s.refreshTTL),
	}); errors.Is(err, sql.ErrNoRows) {
		return TokenPair{}, ErrInvalidToken
	} else if err != nil {
		return TokenPair{}, err
	}

	pair, err := s.issuePair(ctx, qtx, session, now)
	if err != nil {
		return TokenPair{}, err
	}

	if err = tx.Commit(); err != nil {
		return TokenPair{}, fmt.Errorf("commit refresh tx: %w", err)
	}

	return pair, nil
}

// RevokeSession revokes a session and all of its refresh tokens. It is
// idempotent.
func (s *Service) RevokeSession(ctx context.Context, sessionID int64) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin revoke tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err = s.revokeSession(ctx, s.q.WithTx(tx), sessionID); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit revoke tx: %w", err)
	}

	return nil
}

// Authenticate parses an access token and loads the session and user row. The
// session must exist, belong to the token subject, and be neither revoked nor
// expired.
func (s *Service) Authenticate(ctx context.Context, accessToken string) (AuthenticatedSession, error) {
	claims, err := s.jwt.ParseAt(accessToken, s.now())
	if err != nil {
		return AuthenticatedSession{}, err
	}

	session, err := s.q.GetSessionByID(ctx, claims.SessionID)
	if errors.Is(err, sql.ErrNoRows) {
		return AuthenticatedSession{}, ErrInvalidToken
	}
	if err != nil {
		return AuthenticatedSession{}, err
	}

	now := s.now()
	if session.RevokedAt.Valid || !now.Before(session.ExpiresAt) || session.UserEmail != claims.Subject {
		return AuthenticatedSession{}, ErrInvalidToken
	}

	user, err := s.q.GetUserByEmail(ctx, session.UserEmail)
	if errors.Is(err, sql.ErrNoRows) {
		return AuthenticatedSession{}, ErrInvalidToken
	}
	if err != nil {
		return AuthenticatedSession{}, err
	}

	return AuthenticatedSession{Session: session, User: user}, nil
}

func (s *Service) issuePair(ctx context.Context, qtx *dbGen.Queries, session dbGen.Session, now time.Time) (TokenPair, error) {
	accessToken, err := s.jwt.IssueAt(session.UserEmail, session.ID, now, s.accessTTL)
	if err != nil {
		return TokenPair{}, err
	}

	rawRefreshToken, err := securetoken.Generate(refreshTokenBytes)
	if err != nil {
		return TokenPair{}, err
	}

	refreshExpiresAt := now.Add(s.refreshTTL)
	if _, err = qtx.CreateRefreshToken(ctx, dbGen.CreateRefreshTokenParams{
		SessionID: session.ID,
		TokenHash: securetoken.Hash(rawRefreshToken),
		ExpiresAt: refreshExpiresAt,
	}); err != nil {
		return TokenPair{}, err
	}

	return TokenPair{
		UserEmail:        session.UserEmail,
		AccessToken:      accessToken,
		RefreshToken:     rawRefreshToken,
		AccessExpiresIn:  s.accessTTL,
		RefreshExpiresIn: s.refreshTTL,
	}, nil
}

func (s *Service) revokeSession(ctx context.Context, qtx *dbGen.Queries, sessionID int64) error {
	if _, err := qtx.RevokeSession(ctx, sessionID); err != nil {
		return err
	}
	if _, err := qtx.RevokeRefreshTokensBySession(ctx, sessionID); err != nil {
		return err
	}
	return nil
}
