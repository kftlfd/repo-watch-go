package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"repo-watch/internal/securetoken"
)

const (
	// MinJWTSecretLength is the minimum HS256 signing key length in bytes.
	MinJWTSecretLength = 32

	jwtAlgorithm = "HS256"
)

var (
	ErrInvalidToken    = errors.New("invalid token")
	ErrWeakJWTSecret   = errors.New("jwt secret must be at least 32 bytes")
	ErrInvalidTokenTTL = errors.New("token ttl must be positive")
)

// Claims are the registered and application claims carried by an access token.
type Claims struct {
	Subject   string `json:"sub"`
	SessionID int64  `json:"sid"`
	ID        string `json:"jti"`
	IssuedAt  int64  `json:"iat"`
	NotBefore int64  `json:"nbf"`
	ExpiresAt int64  `json:"exp"`
	Issuer    string `json:"iss"`
	Audience  string `json:"aud"`
}

// tokenClaims is the wire representation used by the JWT library. It embeds
// the registered claims so the library can validate exp, nbf, iat, iss, and aud.
type tokenClaims struct {
	SessionID int64 `json:"sid"`
	jwt.RegisteredClaims
}

// JWTManager issues and parses HS256 access tokens.
type JWTManager struct {
	secret   []byte
	issuer   string
	audience string
	leeway   time.Duration
	now      func() time.Time
}

// NewJWTManager returns a manager for HS256 tokens. The secret must be at least
// 32 bytes.
func NewJWTManager(secret []byte, issuer, audience string, leeway time.Duration) (*JWTManager, error) {
	if len(secret) < MinJWTSecretLength {
		return nil, ErrWeakJWTSecret
	}
	if issuer == "" || audience == "" {
		return nil, fmt.Errorf("jwt issuer and audience must not be empty")
	}
	if leeway < 0 {
		leeway = 0
	}

	return &JWTManager{
		secret:   append([]byte(nil), secret...),
		issuer:   issuer,
		audience: audience,
		leeway:   leeway,
		now:      time.Now,
	}, nil
}

// Issue creates a signed access token for subject and sessionID that is valid
// for ttl.
func (m *JWTManager) Issue(subject string, sessionID int64, ttl time.Duration) (string, error) {
	return m.IssueAt(subject, sessionID, m.now(), ttl)
}

// IssueAt creates a signed access token issued at issuedAt and valid for ttl.
func (m *JWTManager) IssueAt(subject string, sessionID int64, issuedAt time.Time, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		return "", ErrInvalidTokenTTL
	}
	if subject == "" || sessionID <= 0 {
		return "", fmt.Errorf("%w: missing subject or session", ErrInvalidToken)
	}

	jti, err := securetoken.Generate(jwtIDBytes)
	if err != nil {
		return "", err
	}

	issuedAt = issuedAt.UTC().Truncate(time.Second)
	claims := tokenClaims{
		SessionID: sessionID,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			ID:        jti,
			IssuedAt:  jwt.NewNumericDate(issuedAt),
			NotBefore: jwt.NewNumericDate(issuedAt),
			ExpiresAt: jwt.NewNumericDate(issuedAt.Add(ttl)),
			Issuer:    m.issuer,
			Audience:  jwt.ClaimStrings{m.audience},
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	signed, err := token.SignedString(m.secret)
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}

	return signed, nil
}

// Parse verifies the token signature and claims and returns the claims.
func (m *JWTManager) Parse(raw string) (Claims, error) {
	return m.ParseAt(raw, m.now())
}

// ParseAt verifies the token signature and claims relative to now.
func (m *JWTManager) ParseAt(raw string, now time.Time) (Claims, error) {
	var parsed tokenClaims

	token, err := jwt.ParseWithClaims(
		raw,
		&parsed,
		func(token *jwt.Token) (any, error) {
			if token.Method.Alg() != jwtAlgorithm {
				return nil, fmt.Errorf("%w: unsupported signing method", ErrInvalidToken)
			}
			return m.secret, nil
		},
		jwt.WithValidMethods([]string{jwtAlgorithm}),
		jwt.WithIssuer(m.issuer),
		jwt.WithAudience(m.audience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(m.leeway),
		jwt.WithTimeFunc(func() time.Time { return now }),
		jwt.WithStrictDecoding(),
	)
	if err != nil {
		return Claims{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !token.Valid {
		return Claims{}, fmt.Errorf("%w: token rejected", ErrInvalidToken)
	}

	claims := Claims{
		Subject:   parsed.Subject,
		SessionID: parsed.SessionID,
		ID:        parsed.ID,
		IssuedAt:  numericUnix(parsed.IssuedAt),
		NotBefore: numericUnix(parsed.NotBefore),
		ExpiresAt: numericUnix(parsed.ExpiresAt),
		Issuer:    parsed.Issuer,
	}
	if len(parsed.Audience) > 0 {
		claims.Audience = parsed.Audience[0]
	}

	if err := m.validateClaims(claims, now); err != nil {
		return Claims{}, err
	}

	return claims, nil
}

func numericUnix(value *jwt.NumericDate) int64 {
	if value == nil {
		return 0
	}
	return value.Unix()
}

func (m *JWTManager) validateClaims(claims Claims, now time.Time) error {
	now = now.UTC()
	if claims.Subject == "" || claims.SessionID <= 0 || claims.ID == "" {
		return fmt.Errorf("%w: missing required claims", ErrInvalidToken)
	}
	if claims.Issuer != m.issuer || claims.Audience != m.audience {
		return fmt.Errorf("%w: invalid issuer or audience", ErrInvalidToken)
	}
	if claims.ExpiresAt == 0 {
		return fmt.Errorf("%w: missing expiration", ErrInvalidToken)
	}

	leeway := int64(m.leeway / time.Second)
	if now.Unix() > claims.ExpiresAt+leeway {
		return fmt.Errorf("%w: expired", ErrInvalidToken)
	}
	if claims.IssuedAt > now.Unix()+leeway {
		return fmt.Errorf("%w: issued in the future", ErrInvalidToken)
	}
	if claims.NotBefore > now.Unix()+leeway {
		return fmt.Errorf("%w: not valid yet", ErrInvalidToken)
	}

	return nil
}
