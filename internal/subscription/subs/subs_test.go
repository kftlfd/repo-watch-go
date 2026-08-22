package subs_test

import (
	"repo-watch/internal/subscription/subs"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	t.Run("consistent with interface", func(t *testing.T) {
		var srv subs.SubsService = subs.NewService(subs.Deps{})
		require.NotNil(t, srv)
	})
}
