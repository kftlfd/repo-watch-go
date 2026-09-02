-- name: CreateOrUpdateSubscription :exec
INSERT INTO subscriptions
    (email, repository_id, active, active_version, unsub_token)
VALUES
    ($1, $2, $3, $4, $5)
ON CONFLICT (email, repository_id) DO UPDATE
SET
    active = EXCLUDED.active,
    active_version = EXCLUDED.active_version,
    unsub_token = EXCLUDED.unsub_token,
    updated_at = now()
WHERE subscriptions.active_version < EXCLUDED.active_version;

-- name: CreateNewReleaseOutboxNotifications :exec
INSERT INTO outbox
    (email, repository_id, owner, name, tag)
SELECT
    subscriptions.email, subscriptions.repository_id, $2, $3, $4
FROM subscriptions
WHERE subscriptions.repository_id = $1
    AND subscriptions.active = true
ON CONFLICT (email, repository_id, tag) DO NOTHING;

-- name: GetNotificationsBatch :many
WITH candidates AS (
    SELECT id
    FROM outbox
    WHERE published_at IS NULL
        AND (leased_until IS NULL OR leased_until < now())
    ORDER BY id ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox
SET leased_until = $2
FROM candidates
WHERE outbox.id = candidates.id
RETURNING outbox.*;

-- name: MarkOutboxPublished :exec
UPDATE outbox
SET published_at = now(),
    leased_until = NULL
WHERE id = ANY(@ids::int[])
    AND published_at IS NULL;
