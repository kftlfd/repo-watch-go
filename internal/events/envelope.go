package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// Envelope is the wire format shared by every producer and consumer.
type Envelope struct {
	EventID       string          `json:"id"`
	EventType     string          `json:"type"`
	Version       int32           `json:"version"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	Payload       json.RawMessage `json:"payload"`
}

// DecodeEnvelope parses an event envelope and verifies that it carries an
// event ID. It does not validate the event type or schema version.
func DecodeEnvelope(body []byte) (Envelope, error) {
	var envelope Envelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return envelope, fmt.Errorf("decode envelope: %w", err)
	}
	if envelope.EventID == "" {
		return envelope, fmt.Errorf("missing event id")
	}
	return envelope, nil
}
