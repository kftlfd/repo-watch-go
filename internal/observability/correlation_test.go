package observability

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCorrelationIDContext(t *testing.T) {
	ctx := context.Background()
	require.Empty(t, GetCorrelationID(ctx))

	ctx = WithCorrelationID(ctx, "corr-1")
	require.Equal(t, "corr-1", GetCorrelationID(ctx))

	require.Equal(t, "corr-1", GetCorrelationID(WithCorrelationID(ctx, "")))
}

func TestNormalizeCorrelationID(t *testing.T) {
	require.Equal(t, "corr-1", NormalizeCorrelationID("  corr-1  "))
	require.Equal(t, "scan:2026-11-23_abc.1", NormalizeCorrelationID("scan:2026-11-23_abc.1"))
	require.Empty(t, NormalizeCorrelationID(""))
	require.Empty(t, NormalizeCorrelationID(strings.Repeat("a", maxCorrelationIDLength+1)))
	require.Empty(t, NormalizeCorrelationID("corr\x00id"))
	require.Empty(t, NormalizeCorrelationID("corr id"))
	require.Empty(t, NormalizeCorrelationID("corr+\u00e9"))
}

func TestNewCorrelationID(t *testing.T) {
	id := NewCorrelationID()

	require.Len(t, id, 32)
	require.NotEqual(t, id, NewCorrelationID())
}
