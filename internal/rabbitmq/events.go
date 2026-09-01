package rabbitmq

import (
	"encoding/json"
	"time"
)

type EnvelopeJSON struct {
	EventID   string          `json:"id"`
	EventType string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

//
// repository-changed
//

const EventRepositoryChanged = "repository.changed"

type EventRepositoryChangedPayload struct {
	RepositoryID       string    `json:"repository_id"`
	Owner              string    `json:"owner"`
	Name               string    `json:"name"`
	Active             bool      `json:"active"`
	ActiveStateVersion int32     `json:"active_state_version"`
	UpdatedAt          time.Time `json:"updated_at"`
}

//
// subscription-changed
//

const EventSubscriptionChanged = "subscription.changed"

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

type EventReleaseDetectedPayload struct {
	RepositoryID string `json:"repository_id"`
	ReleaseID    string `json:"release_id"`
}

//
// email-queued
//

const EventEmailQueued = "email.queued"

type EventEmailQueuedPayload struct {
	Recipient string `json:"recipient"`
	Type      string `json:"type"`
	Data      any    `json:"data"`
}
