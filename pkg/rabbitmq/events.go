package rabbitmq

import (
	"encoding/json"
	"time"
)

type envelopeJSON struct {
	EventID     string          `json:"id"`
	MessageType string          `json:"msg_type"`
	CreatedAt   time.Time       `json:"created_at"`
	Payload     json.RawMessage `json:"payload"`
}

type event interface {
	event()
}

type eventDescriptor[T event] struct {
	messageType string
	routingKey  string
}

//
// repository-changed
//

var EventRepositoryChanged = eventDescriptor[EvRepositoryChanged]{
	messageType: "repository.changed",
	routingKey:  queueRepositoryChanged,
}

type EvRepositoryChanged struct {
	RepositoryID string `json:"repository_id"`
}

func (EvRepositoryChanged) event() {}

//
// subscription-changed
//

var EventSubscriptionChanged = eventDescriptor[EvSubscriptionChanged]{
	messageType: "subscription.changed",
	routingKey:  queueSubscriptionChanged,
}

type EvSubscriptionChanged struct {
	SubscriptionID string `json:"subscription_id"`
}

func (EvSubscriptionChanged) event() {}

//
// release-detected
//

var EventReleaseDetected = eventDescriptor[EvReleaseDetected]{
	messageType: "release.detected",
	routingKey:  queueReleaseDetected,
}

type EvReleaseDetected struct {
	RepositoryID string `json:"repository_id"`
	ReleaseID    string `json:"release_id"`
}

func (EvReleaseDetected) event() {}

//
// email-queued
//

var EventEmailQueued = eventDescriptor[EvEmailQueued]{
	messageType: "email.queued",
	routingKey:  queueEmailQueued,
}

type EvEmailQueued struct {
	Recipient string `json:"recipient"`
}

func (EvEmailQueued) event() {}
