package mailer

import (
	"encoding/json"
	"testing"

	"repo-watch/internal/emails"
	"repo-watch/internal/rabbitmq"

	"github.com/stretchr/testify/require"
)

func TestRenderEmail(t *testing.T) {
	tests := []struct {
		name        string
		emailType   string
		data        any
		wantSubject string
		wantBody    string
	}{
		{
			name:      "confirm subscription",
			emailType: emails.EmailConfirmSubscription,
			data: emails.EmailConfirmSubscriptionData{
				RepoID:       "owner/repository",
				ConfirmToken: "confirm-token",
			},
			wantSubject: "Confirm subscription to: owner/repository",
			wantBody:    "confirm-token",
		},
		{
			name:      "subscribed",
			emailType: emails.EmailSubscribed,
			data: emails.EmailSubscribedData{
				RepoID: "owner/repository",
			},
			wantSubject: "You subscribed to: owner/repository",
			wantBody:    "owner/repository",
		},
		{
			name:      "new release",
			emailType: emails.EmailNewReleaseNotification,
			data: emails.EmailNewReleaseNotificationPayload{
				RepoID:     "owner/repository",
				Owner:      "owner",
				Name:       "repository",
				Tag:        "v1.2.3",
				UnsubToken: "unsub-token",
			},
			wantSubject: "New release for owner/repository: v1.2.3",
			wantBody:    "unsub-token",
		},
		{
			name:      "unsubscribed",
			emailType: emails.EmailUnsubscribed,
			data: emails.EmailUnsubscribedData{
				RepoID: "owner/repository",
			},
			wantSubject: "Unsubscribed from: owner/repository",
			wantBody:    "owner/repository",
		},
		{
			name:      "repository removed",
			emailType: emails.EmailRepositoryRemoved,
			data: emails.EmailRepositoryRemovedPayload{
				RepoID: "owner/repository",
			},
			wantSubject: "Repository was removed: owner/repository",
			wantBody:    "owner/repository",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.data)
			require.NoError(t, err)

			email, err := renderEmail(rabbitmq.EventEmailQueuedPayload{
				Recipient: "alice@example.com",
				Type:      tt.emailType,
				Data:      data,
			})
			require.NoError(t, err)
			require.Equal(t, tt.wantSubject, email.subject)
			require.Contains(t, email.body, tt.wantBody)
		})
	}
}

func TestRenderEmailRejectsInvalidData(t *testing.T) {
	_, err := renderEmail(rabbitmq.EventEmailQueuedPayload{
		Type: emails.EmailRepositoryRemoved,
		Data: json.RawMessage(`"not-an-object"`),
	})
	require.ErrorIs(t, err, ErrInvalidPayloadData)
}

func TestRenderEmailRejectsUnknownType(t *testing.T) {
	_, err := renderEmail(rabbitmq.EventEmailQueuedPayload{
		Type: "unknown-email-type",
		Data: json.RawMessage(`{}`),
	})
	require.ErrorContains(t, err, `unexpected email type: "unknown-email-type"`)
}
