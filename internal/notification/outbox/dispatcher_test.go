package outbox

import (
	"encoding/json"
	"testing"
	"time"

	"repo-watch/internal/emails"
	"repo-watch/internal/notification/database"
	"repo-watch/internal/rabbitmq"

	"github.com/stretchr/testify/require"
)

func TestMapOutboxRowRepoRemoved(t *testing.T) {
	row := database.Outbox{
		ID:           1,
		Type:         "repo_removed",
		Email:        "alice@example.com",
		RepositoryID: "owner/repository",
		Tag:          "repo_removed:event-1",
		UnsubToken:   "",
		Payload:      json.RawMessage(`{}`),
		CreatedAt:    time.Now(),
	}

	got, err := mapOutboxRow(row)
	require.NoError(t, err)
	require.Equal(t, rabbitmq.EventEmailQueued, got.EventType)
	require.Equal(t, int32(1), got.ID)

	var event rabbitmq.EventEmailQueuedPayload
	require.NoError(t, json.Unmarshal(got.Payload, &event))
	require.Equal(t, "alice@example.com", event.Recipient)
	require.Equal(t, emails.EmailRepositoryRemoved, event.Type)

	var data emails.EmailRepositoryRemovedPayload
	require.NoError(t, json.Unmarshal(event.Data, &data))
	require.Equal(t, "owner/repository", data.RepoID)
}

func TestMapOutboxRowNewRelease(t *testing.T) {
	row := database.Outbox{
		ID:           2,
		Type:         "new_release",
		Email:        "bob@example.com",
		RepositoryID: "owner/repository",
		Tag:          "v1.2.3",
		UnsubToken:   "unsub-token",
		Payload:      json.RawMessage(`{"owner":"owner","name":"repository"}`),
		CreatedAt:    time.Now(),
	}

	got, err := mapOutboxRow(row)
	require.NoError(t, err)
	require.Equal(t, rabbitmq.EventEmailQueued, got.EventType)

	var event rabbitmq.EventEmailQueuedPayload
	require.NoError(t, json.Unmarshal(got.Payload, &event))
	require.Equal(t, "bob@example.com", event.Recipient)
	require.Equal(t, emails.EmailNewReleaseNotification, event.Type)

	var data emails.EmailNewReleaseNotificationPayload
	require.NoError(t, json.Unmarshal(event.Data, &data))
	require.Equal(t, "owner/repository", data.RepoID)
	require.Equal(t, "owner", data.Owner)
	require.Equal(t, "repository", data.Name)
	require.Equal(t, "v1.2.3", data.Tag)
	require.Equal(t, "unsub-token", data.UnsubToken)
}

func TestMapOutboxRowUnknownType(t *testing.T) {
	_, err := mapOutboxRow(database.Outbox{Type: "unknown"})
	require.ErrorContains(t, err, `unexpected outbox event type: "unknown"`)
}
