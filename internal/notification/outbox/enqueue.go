package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"repo-watch/internal/notification/db/dbGen"
	"repo-watch/internal/observability"
)

// NewReleaseNotifications creates one notification row per active subscriber
// of repositoryID in the current transaction.
func NewReleaseNotifications(
	ctx context.Context,
	qtx *dbGen.Queries,
	repositoryID, tag, owner, name string,
) error {
	payload, err := json.Marshal(NewReleasePayload{
		Owner: owner,
		Name:  name,
	})
	if err != nil {
		return fmt.Errorf("marshal new release payload: %w", err)
	}

	correlationID := observability.GetCorrelationID(ctx)
	traceParent, _ := observability.GetTraceParent(ctx)

	if err := qtx.CreateNewReleaseOutboxNotifications(ctx, dbGen.CreateNewReleaseOutboxNotificationsParams{
		RepositoryID:  repositoryID,
		Tag:           tag,
		Payload:       payload,
		CorrelationID: correlationID,
		TraceParent:   traceParent,
	}); err != nil {
		return fmt.Errorf("create new release notifications: %w", err)
	}

	return nil
}

// RepositoryRemovedNotifications deactivates subscriptions for repositoryID
// and creates one removal notification row per subscriber in the current
// transaction.
func RepositoryRemovedNotifications(
	ctx context.Context,
	qtx *dbGen.Queries,
	repositoryID, eventID string,
) error {
	payload, err := json.Marshal(RepoRemovedPayload{})
	if err != nil {
		return fmt.Errorf("marshal repository removed payload: %w", err)
	}

	correlationID := observability.GetCorrelationID(ctx)
	traceParent, _ := observability.GetTraceParent(ctx)

	if err := qtx.DeactivateSubsForRepoAndNotifySubs(ctx, dbGen.DeactivateSubsForRepoAndNotifySubsParams{
		RepositoryID:  repositoryID,
		Tag:           "repo_removed:" + eventID,
		Payload:       payload,
		CorrelationID: correlationID,
		TraceParent:   traceParent,
	}); err != nil {
		return fmt.Errorf("deactivate repository subscriptions: %w", err)
	}

	return nil
}
