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
	RepositoryID string `json:"repository_id"`
}

//
// subscription-changed
//

const EventSubscriptionChanged = "subscription.changed"

type EventSubscriptionChangedPayload struct {
	SubscriptionID string `json:"subscription_id"`
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
	Subject   string `json:"subject"`
}
