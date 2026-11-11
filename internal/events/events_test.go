package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAcceptsLegacyAndCurrentVersions(t *testing.T) {
	for _, version := range []int32{0, LegacyVersion, VersionEmailQueued} {
		require.NoError(t, Validate(EventEmailQueued, version))
	}
}

func TestValidateRejectsNewerVersion(t *testing.T) {
	err := Validate(EventEmailQueued, VersionEmailQueued+1)
	require.ErrorContains(t, err, "unsupported version")
}

func TestValidateRejectsUnknownType(t *testing.T) {
	err := Validate("unknown.event", 1)
	require.ErrorContains(t, err, "unknown event type")
}

func TestValidateRejectsNegativeVersion(t *testing.T) {
	err := Validate(EventEmailQueued, -1)
	require.ErrorContains(t, err, "invalid version")
}

func TestCurrentVersionIsSupported(t *testing.T) {
	for _, eventType := range []string{
		EventRepositoryChanged,
		EventRepositoryRemoved,
		EventSubscriptionChanged,
		EventReleaseDetected,
		EventEmailQueued,
	} {
		version, ok := CurrentVersion(eventType)
		require.True(t, ok, eventType)
		require.GreaterOrEqual(t, version, LegacyVersion, eventType)
		require.NoError(t, Validate(eventType, version), eventType)
	}
}
