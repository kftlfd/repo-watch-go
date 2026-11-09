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
    (type, email, repository_id, tag, unsub_token, payload)
SELECT
    'new_release', subscriptions.email, subscriptions.repository_id, $2, subscriptions.unsub_token, $3
FROM subscriptions
WHERE subscriptions.repository_id = $1
    AND subscriptions.active = true
ON CONFLICT (email, repository_id, tag) DO NOTHING;

-- name: GetNotificationsBatch :many
WITH candidates AS (
    SELECT id
    FROM outbox
    WHERE published_at IS NULL
        AND failed_at IS NULL
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

-- name: RecordOutboxFailure :one
UPDATE outbox
SET
    attempts = attempts + 1,
    last_error = @last_error,
    failed_at = CASE
        WHEN @terminal::boolean OR attempts + 1 >= @max_attempts
        THEN now()
        ELSE NULL
    END,
    leased_until = CASE
        WHEN @terminal::boolean OR attempts + 1 >= @max_attempts
        THEN NULL
        ELSE leased_until
    END
WHERE id = @id
    AND published_at IS NULL
    AND failed_at IS NULL
RETURNING attempts, failed_at;

-- name: DeactivateSubsForRepoAndNotifySubs :exec
WITH subs AS (
    UPDATE subscriptions
    SET
        active = false,
        unsub_token = '',
        updated_at = now()
    WHERE repository_id = $1
        AND active = true
    RETURNING email, unsub_token
)
INSERT INTO outbox
    (type, email, repository_id, tag, unsub_token, payload)
SELECT
    'repo_removed', subs.email, $1, $2, subs.unsub_token, $3
FROM subs;
