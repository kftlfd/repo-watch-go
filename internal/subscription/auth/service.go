package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"repo-watch/internal/emails"
	"repo-watch/internal/events"
	"repo-watch/internal/securetoken"
	"repo-watch/internal/subscription/db/dbGen"
	"repo-watch/internal/subscription/identity"
	"repo-watch/internal/subscription/outbox"
)

const (
	// MinPasswordLength is the default minimum accepted password length in bytes.
	MinPasswordLength = 1

	// MaxPasswordLength is the default maximum accepted password length in bytes.
	MaxPasswordLength = 1024

	// DefaultIssuer and DefaultAudience identify tokens issued by this service.
	DefaultIssuer   = "repo-watch"
	DefaultAudience = "repo-watch-api"

	// DefaultAccessTTL is the lifetime of an access token.
	DefaultAccessTTL = 10 * time.Minute

	// DefaultRefreshTTL is the sliding lifetime of a refresh token and session.
	DefaultRefreshTTL = 30 * 24 * time.Hour

	// DefaultJWTLeeway tolerates small clock skew when validating JWT timestamps.
	DefaultJWTLeeway = 30 * time.Second
)

var (
	ErrInvalidCredentials    = errors.New("invalid credentials")
	ErrEmailUnverified       = errors.New("email not verified")
	ErrInvalidPasswordLength = errors.New("invalid password length")
)

// dummyPasswordHash is verified for unknown accounts so that login takes
// roughly the same time whether or not the email exists.
var dummyPasswordHash = func() string {
	hash, err := HashPassword("invalid-password-placeholder")
	if err != nil {
		panic(fmt.Sprintf("create dummy password hash: %v", err))
	}
	return hash
}()

// Config controls token lifetimes, password validation, and JWT validation.
type Config struct {
	JWTSecret  []byte
	Issuer     string
	Audience   string
	AccessTTL  time.Duration
	RefreshTTL time.Duration
	Leeway     time.Duration
	Now        func() time.Time

	MinPasswordLength int
	MaxPasswordLength int
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
	db                *sql.DB
	q                 *dbGen.Queries
	jwt               *JWTManager
	accessTTL         time.Duration
	refreshTTL        time.Duration
	minPasswordLength int
	maxPasswordLength int
	now               func() time.Time
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
		cfg.Leeway = DefaultJWTLeeway
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MinPasswordLength <= 0 {
		cfg.MinPasswordLength = MinPasswordLength
	}
	if cfg.MaxPasswordLength <= 0 {
		cfg.MaxPasswordLength = MaxPasswordLength
	}
	if cfg.MinPasswordLength > cfg.MaxPasswordLength {
		return nil, fmt.Errorf(
			"min password length %d exceeds max %d",
			cfg.MinPasswordLength,
			cfg.MaxPasswordLength,
		)
	}

	jwt, err := NewJWTManager(cfg.JWTSecret, cfg.Issuer, cfg.Audience, cfg.Leeway)
	if err != nil {
		return nil, err
	}

	return &Service{
		db:                d.DB,
		q:                 d.Queries,
		jwt:               jwt,
		accessTTL:         cfg.AccessTTL,
		refreshTTL:        cfg.RefreshTTL,
		minPasswordLength: cfg.MinPasswordLength,
		maxPasswordLength: cfg.MaxPasswordLength,
		now:               cfg.Now,
	}, nil
}

func (s *Service) validatePasswordLength(password string) error {
	if len(password) < s.minPasswordLength || len(password) > s.maxPasswordLength {
		return ErrInvalidPasswordLength
	}
	return nil
}

// CreateSession creates a session and refresh token for an existing user and
// returns a new token pair. Callers must enforce email verification; Login is
// currently the verification boundary, and Authenticate trusts created sessions.
func (s *Service) CreateSession(ctx context.Context, email identity.Email) (TokenPair, error) {
	now := s.now()
	expiresAt := now.Add(s.refreshTTL)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return TokenPair{}, fmt.Errorf("begin session tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	session, err := qtx.CreateSession(ctx, dbGen.CreateSessionParams{
		UserEmail: email.String(),
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

// Signup creates an unverified user and queues a verification email. Existing
// unverified users get an updated password, a rotated token, and a new email.
// Existing verified users are left alone; callers must return the same generic
// response for every case.
func (s *Service) Signup(ctx context.Context, email identity.Email, password string) error {
	if err := s.validatePasswordLength(password); err != nil {
		return err
	}

	passwordHash, err := HashPassword(password)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin signup tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	_, err = qtx.CreateUser(ctx, dbGen.CreateUserParams{
		Email:        email.String(),
		PasswordHash: passwordHash,
	})
	switch {
	case err == nil:
	case errors.Is(err, sql.ErrNoRows):
		user, lookupErr := qtx.GetUserByEmailForUpdate(ctx, email.String())
		if errors.Is(lookupErr, sql.ErrNoRows) {
			return nil
		}
		if lookupErr != nil {
			return fmt.Errorf("load existing user: %w", lookupErr)
		}
		if user.EmailVerifiedAt.Valid {
			return nil
		}
		if _, err = qtx.UpdateUnverifiedUserPasswordHash(ctx, dbGen.UpdateUnverifiedUserPasswordHashParams{
			Email:        email.String(),
			PasswordHash: passwordHash,
		}); err != nil {
			return fmt.Errorf("update unverified user password: %w", err)
		}
	default:
		return fmt.Errorf("create user: %w", err)
	}

	if err = s.queueVerificationEmail(ctx, qtx, email); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit signup tx: %w", err)
	}

	return nil
}

// VerifyEmail consumes a verification token and marks the user's email as
// verified. Invalid and expired tokens both return ErrInvalidToken.
func (s *Service) VerifyEmail(ctx context.Context, token string) error {
	if !securetoken.Validate(token, userTokenVerifyBytes) {
		return ErrInvalidToken
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin verify tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	tokenHash := securetoken.Hash(token)
	candidate, err := qtx.FindUserToken(ctx, dbGen.FindUserTokenParams{
		TokenType: UserTokenTypeVerify,
		TokenHash: tokenHash,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidToken
	}
	if err != nil {
		return fmt.Errorf("find verification token: %w", err)
	}

	if _, err = qtx.GetUserByEmailForUpdate(ctx, candidate.UserEmail); errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidToken
	} else if err != nil {
		return fmt.Errorf("lock verification user: %w", err)
	}

	userToken, err := qtx.FindUserTokenForUpdate(ctx, dbGen.FindUserTokenForUpdateParams{
		TokenType: UserTokenTypeVerify,
		TokenHash: tokenHash,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidToken
	}
	if err != nil {
		return fmt.Errorf("lock verification token: %w", err)
	}
	if userToken.UserEmail != candidate.UserEmail {
		return ErrInvalidToken
	}

	if !s.now().Before(userToken.CreatedAt.Add(UserTokenVerifyTTL)) {
		return ErrInvalidToken
	}

	if _, err = qtx.MarkUserEmailVerified(ctx, userToken.UserEmail); errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidToken
	} else if err != nil {
		return fmt.Errorf("mark email verified: %w", err)
	}

	if err = qtx.DeleteUserToken(ctx, userToken.ID); err != nil {
		return fmt.Errorf("consume verification token: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit verify tx: %w", err)
	}

	return nil
}

// ResendVerification rotates and resends a verification token for an existing
// unverified user. Unknown or verified users are ignored so the response does
// not reveal whether an account exists.
func (s *Service) ResendVerification(ctx context.Context, email identity.Email) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin resend tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	user, err := qtx.GetUserByEmailForUpdate(ctx, email.String())
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load user: %w", err)
	}
	if user.EmailVerifiedAt.Valid {
		return nil
	}

	if err = s.queueVerificationEmail(ctx, qtx, email); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit resend tx: %w", err)
	}

	return nil
}

// Login verifies a password and creates a session for a verified user.
// Unverified users are rejected only after their password has been checked.
func (s *Service) Login(ctx context.Context, email identity.Email, password string) (TokenPair, error) {
	if err := s.validatePasswordLength(password); err != nil {
		return TokenPair{}, ErrInvalidCredentials
	}

	user, err := s.q.GetUserByEmail(ctx, email.String())
	if errors.Is(err, sql.ErrNoRows) {
		_, _ = VerifyPassword(dummyPasswordHash, password)
		return TokenPair{}, ErrInvalidCredentials
	}
	if err != nil {
		return TokenPair{}, fmt.Errorf("load user: %w", err)
	}

	ok, err := VerifyPassword(user.PasswordHash, password)
	if err != nil {
		return TokenPair{}, fmt.Errorf("verify password: %w", err)
	}
	if !ok {
		return TokenPair{}, ErrInvalidCredentials
	}
	if !user.EmailVerifiedAt.Valid {
		return TokenPair{}, ErrEmailUnverified
	}

	return s.CreateSession(ctx, email)
}

// lockSessionAndRefreshToken locks the parent session before the token. The
// unlocked candidate lookup only discovers the session; the locked token must
// still match it before either refresh or logout can proceed.
func lockSessionAndRefreshToken(
	ctx context.Context,
	qtx *dbGen.Queries,
	tokenHash string,
) (dbGen.Session, dbGen.RefreshToken, error) {
	candidate, err := qtx.GetRefreshTokenByHash(ctx, tokenHash)
	if err != nil {
		return dbGen.Session{}, dbGen.RefreshToken{}, err
	}

	session, err := qtx.GetSessionByIDForUpdate(ctx, candidate.SessionID)
	if err != nil {
		return dbGen.Session{}, dbGen.RefreshToken{}, err
	}

	refreshToken, err := qtx.GetRefreshTokenByHashForUpdate(ctx, tokenHash)
	if err != nil {
		return dbGen.Session{}, dbGen.RefreshToken{}, err
	}
	if refreshToken.SessionID != session.ID {
		return dbGen.Session{}, dbGen.RefreshToken{}, sql.ErrNoRows
	}

	return session, refreshToken, nil
}

// RevokeSessionByRefreshToken revokes the session that owns a refresh token.
// It is used by logout when the access token has already expired.
func (s *Service) RevokeSessionByRefreshToken(ctx context.Context, rawRefreshToken string) error {
	if !securetoken.Validate(rawRefreshToken, refreshTokenBytes) {
		return ErrInvalidToken
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin logout tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := s.q.WithTx(tx)

	_, refreshToken, err := lockSessionAndRefreshToken(ctx, qtx, securetoken.Hash(rawRefreshToken))
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidToken
	}
	if err != nil {
		return fmt.Errorf("lock session refresh token: %w", err)
	}

	if err = s.revokeSession(ctx, qtx, refreshToken.SessionID); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit logout tx: %w", err)
	}

	return nil
}

func (s *Service) queueVerificationEmail(ctx context.Context, qtx *dbGen.Queries, email identity.Email) error {
	rawToken, tokenHash, err := NewUserVerifyToken()
	if err != nil {
		return fmt.Errorf("generate verification token: %w", err)
	}

	if _, err = qtx.UpsertUserToken(ctx, dbGen.UpsertUserTokenParams{
		UserEmail: email.String(),
		TokenType: UserTokenTypeVerify,
		TokenHash: tokenHash,
	}); err != nil {
		return fmt.Errorf("store verification token: %w", err)
	}

	data, err := json.Marshal(emails.EmailVerifyAccountData{VerifyToken: rawToken})
	if err != nil {
		return fmt.Errorf("marshal verification email: %w", err)
	}

	if err = outbox.NewEmail(ctx, qtx, events.EventEmailQueuedPayload{
		Recipient: email.String(),
		Type:      emails.EmailVerifyAccount,
		Data:      data,
	}); err != nil {
		return fmt.Errorf("queue verification email: %w", err)
	}

	return nil
}

// RefreshSession rotates a refresh token and returns a new pair. Used refresh
// tokens are invalid and do not revoke the session created by a later rotation.
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

	session, refreshToken, err := lockSessionAndRefreshToken(ctx, qtx, securetoken.Hash(rawRefreshToken))
	if errors.Is(err, sql.ErrNoRows) {
		return TokenPair{}, ErrInvalidToken
	}
	if err != nil {
		return TokenPair{}, err
	}

	now := s.now()

	if refreshToken.UsedAt.Valid {
		return TokenPair{}, ErrInvalidToken
	}

	if refreshToken.RevokedAt.Valid || !now.Before(refreshToken.ExpiresAt) {
		return TokenPair{}, ErrInvalidToken
	}

	if session.RevokedAt.Valid || !now.Before(session.ExpiresAt) {
		return TokenPair{}, ErrInvalidToken
	}

	if _, err = qtx.MarkRefreshTokenUsed(ctx, refreshToken.ID); errors.Is(err, sql.ErrNoRows) {
		return TokenPair{}, ErrInvalidToken
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
