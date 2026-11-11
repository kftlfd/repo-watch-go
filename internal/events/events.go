// Package events defines the JSON contracts shared between services: the
// envelope, the event types, their payloads, and the schema versions.
//
// Compatibility policy:
//   - Every event type has a schema version that starts at 1 and only increases.
//   - Additive payload changes that older consumers can ignore do not bump the
//     version. Removing, renaming, or changing the meaning of a field does.
//   - Consumers accept versions up to the one this package declares. A missing
//     version is treated as the first version, for messages produced before
//     versioning existed.
//   - A consumer that sees an unsupported newer version rejects the message to
//     the dead-letter queue instead of guessing at the schema.
//
// The version of a persisted event is stored next to its payload in the
// outbox, so rows written before an upgrade keep the version they were written
// with even when a newer dispatcher publishes them.
package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// LegacyVersion is the version assumed when an envelope has no version field.
// All events produced before schema versioning are version 1.
const LegacyVersion int32 = 1

// Envelope is the wire format shared by every producer and consumer.
type Envelope struct {
	EventID   string          `json:"id"`
	EventType string          `json:"type"`
	Version   int32           `json:"version"`
	CreatedAt time.Time       `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

//
// repository-changed
//

const EventRepositoryChanged = "repository.changed"

const VersionRepositoryChanged int32 = 1

type EventRepositoryChangedPayload struct {
	RepositoryID  string    `json:"repository_id"`
	Owner         string    `json:"owner"`
	Name          string    `json:"name"`
	LatestSeenTag string    `json:"latest_seen_tag"`
	Active        bool      `json:"active"`
	ActiveVersion int32     `json:"active_version"`
	UpdatedAt     time.Time `json:"updated_at"`
}

//
// repository-removed
//

const EventRepositoryRemoved = "repository.removed"

const VersionRepositoryRemoved int32 = 1

type EventRepositoryRemovedPayload struct {
	RepositoryID string `json:"repository_id"`
	Owner        string `json:"owner"`
	Name         string `json:"name"`
}

//
// subscription-changed
//

const EventSubscriptionChanged = "subscription.changed"

const VersionSubscriptionChanged int32 = 1

type EventSubscriptionChangedPayload struct {
	SubscriptionID int32     `json:"subscription_id"`
	Email          string    `json:"email"`
	RepositoryID   string    `json:"repository_id"`
	Active         bool      `json:"active"`
	Version        int32     `json:"version"`
	UnsubToken     string    `json:"unsub_token"`
	UpdatedAt      time.Time `json:"updated_at"`
}

//
// release-detected
//

const EventReleaseDetected = "release.detected"

const VersionReleaseDetected int32 = 1

type EventReleaseDetectedPayload struct {
	RepositoryID string `json:"repository_id"`
	Owner        string `json:"owner"`
	Name         string `json:"name"`
	Tag          string `json:"tag"`
}

//
// email-queued
//

const EventEmailQueued = "email.queued"

const VersionEmailQueued int32 = 1

type EventEmailQueuedPayload struct {
	Recipient string          `json:"recipient"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
}

var currentVersions = map[string]int32{
	EventRepositoryChanged:   VersionRepositoryChanged,
	EventRepositoryRemoved:   VersionRepositoryRemoved,
	EventSubscriptionChanged: VersionSubscriptionChanged,
	EventReleaseDetected:     VersionReleaseDetected,
	EventEmailQueued:         VersionEmailQueued,
}

// CurrentVersion returns the schema version this package produces for an event
// type.
func CurrentVersion(eventType string) (int32, bool) {
	version, ok := currentVersions[eventType]
	return version, ok
}

// NormalizeVersion maps the zero value to LegacyVersion. Any other value is
// returned unchanged.
func NormalizeVersion(version int32) int32 {
	if version < LegacyVersion {
		return LegacyVersion
	}
	return version
}

// Validate reports whether eventType is known and version is one this package
// supports. An unset version is accepted as LegacyVersion.
func Validate(eventType string, version int32) error {
	current, ok := currentVersions[eventType]
	if !ok {
		return fmt.Errorf("unknown event type %q", eventType)
	}
	if version < 0 {
		return fmt.Errorf("event %q: invalid version %d", eventType, version)
	}

	version = NormalizeVersion(version)
	if version > current {
		return fmt.Errorf(
			"event %q: unsupported version %d (max supported %d)",
			eventType, version, current)
	}

	return nil
}
