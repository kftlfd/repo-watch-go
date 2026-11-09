package inbox

import (
	"database/sql"
	"encoding/json"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"repo-watch/internal/rabbitmq"
	"repo-watch/internal/subscription/db/dbGen"
)

func TestHandlerDiscardsInvalidPayload(t *testing.T) {
	w := &inboxWorker{}

	ack, err := w.handler(rabbitmq.EnvelopeJSON{
		EventType: rabbitmq.EventRepositoryRemoved,
		Payload:   json.RawMessage(`not-json`),
	})

	require.Equal(t, rabbitmq.NackDiscard, ack)
	require.Error(t, err)
}

func TestHandlerRequeuesTransientDatabaseFailure(t *testing.T) {
	db, err := sql.Open("postgres", "")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	w := &inboxWorker{
		db: db,
		q:  dbGen.New(db),
	}

	ack, err := w.handler(rabbitmq.EnvelopeJSON{
		EventType: rabbitmq.EventRepositoryRemoved,
		Payload: json.RawMessage(`{
			"repository_id": "owner/repo",
			"owner": "owner",
			"name": "repo"
		}`),
	})

	require.Equal(t, rabbitmq.NackRequeue, ack)
	require.Error(t, err)
}
