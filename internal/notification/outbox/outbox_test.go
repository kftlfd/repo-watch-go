package outbox

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"repo-watch/internal/emails"
	"repo-watch/internal/notification/database"
	"repo-watch/internal/rabbitmq"
)

func TestGetPayloadRepoRemoved(t *testing.T) {
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

	evType, evPayload, err := getEventPayload(row)
	require.NoError(t, err)
	require.Equal(t, rabbitmq.EventEmailQueued, evType)

	var event rabbitmq.EventEmailQueuedPayload
	require.NoError(t, json.Unmarshal(evPayload, &event))
	require.Equal(t, "alice@example.com", event.Recipient)
	require.Equal(t, emails.EmailRepositoryRemoved, event.Type)

	var data emails.EmailRepositoryRemovedPayload
	require.NoError(t, json.Unmarshal(event.Data, &data))
	require.Equal(t, "owner/repository", data.RepoID)
}

func TestGetPayloadNewRelease(t *testing.T) {
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

	evType, evPayload, err := getEventPayload(row)
	require.NoError(t, err)
	require.Equal(t, rabbitmq.EventEmailQueued, evType)

	var event rabbitmq.EventEmailQueuedPayload
	require.NoError(t, json.Unmarshal(evPayload, &event))
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

func TestGetPayloadUnknownType(t *testing.T) {
	_, _, err := getEventPayload(database.Outbox{Type: "unknown"})
	require.ErrorContains(t, err, "unknown")
}
