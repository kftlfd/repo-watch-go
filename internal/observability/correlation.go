package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

const maxCorrelationIDLength = 32

type correlationIDKey struct{}

// NormalizeCorrelationID trims and accepts only a-zA-Z0-9-_.: chars.
// Callers should generate a new ID when it returns an empty string.
func NormalizeCorrelationID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > maxCorrelationIDLength {
		return ""
	}

	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.', r == ':':
		default:
			return ""
		}
	}

	return id
}

// Returns a new context with the correlationID attached
func WithCorrelationID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, correlationIDKey{}, id)
}

// Get the correlationID from context
func GetCorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey{}).(string)
	return id
}

func NewCorrelationID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}

	return hex.EncodeToString(buf[:])
}
