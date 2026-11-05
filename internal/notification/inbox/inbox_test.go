package inbox

import (
	"database/sql"
	"encoding/json"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"repo-watch/internal/notification/database"
	"repo-watch/internal/rabbitmq"
)

func TestHandlerDiscardsInvalidPayload(t *testing.T) {
	w := &inboxWorker{}

	for _, eventType := range []string{
		rabbitmq.EventSubscriptionChanged,
		rabbitmq.EventReleaseDetected,
		rabbitmq.EventRepositoryRemoved,
	} {
		t.Run(eventType, func(t *testing.T) {
			ack, err := w.handler(rabbitmq.EnvelopeJSON{
				EventType: eventType,
				Payload:   json.RawMessage(`not-json`),
			})

			require.Equal(t, rabbitmq.NackDiscard, ack)
			require.Error(t, err)
		})
	}
}

func TestHandlerRequeuesTransientDatabaseFailure(t *testing.T) {
	db, err := sql.Open("postgres", "")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	w := &inboxWorker{q: database.New(db)}

	ack, err := w.handler(rabbitmq.EnvelopeJSON{
		EventType: rabbitmq.EventSubscriptionChanged,
		Payload: json.RawMessage(`{
			"email": "alice@example.com",
			"repository_id": "owner/repo",
			"active": true,
			"version": 1,
			"unsub_token": "token"
		}`),
	})

	require.Equal(t, rabbitmq.NackRequeue, ack)
	require.Error(t, err)
}
