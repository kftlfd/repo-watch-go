-- name: CreateSubscription :one
INSERT INTO subscriptions
    (email, repository_id)
VALUES
    ($1, $2)
ON CONFLICT (email, repository_id) DO NOTHING
RETURNING *;

-- name: GetSubscription :one
SELECT *
FROM subscriptions
WHERE email = $1
    AND repository_id = $2;

-- name: GetSubscriptionByID :one
SELECT *
FROM subscriptions
WHERE id = $1;

-- name: ActivateSubscription :one
UPDATE subscriptions
SET
    active = true,
    active_version = CASE
        WHEN active = false THEN active_version + 1
        ELSE active_version
    END,
    updated_at = now()
WHERE id = $1
    AND active = false
RETURNING *;

-- name: DeactivateSubscription :one
UPDATE subscriptions
SET
    active = false,
    active_version = CASE
        WHEN active = true THEN active_version + 1
        ELSE active_version
    END,
    updated_at = now()
WHERE id = $1
    AND active = true
RETURNING *;

-- name: ListSubscriptionsByEmail :many
SELECT
    repositories.id
FROM subscriptions
    JOIN repositories ON subscriptions.repository_id = repositories.id
WHERE subscriptions.email = $1
    AND subscriptions.active = true
ORDER BY subscriptions.created_at ASC;

-- name: DeactivateSubsForRepoAndRemoveTokens :exec
WITH removed_subs as (
    UPDATE subscriptions
    SET
        active = false,
        active_version = active_version + 1,
        updated_at = now()
    WHERE repository_id = $1
        AND active = true
    RETURNING id
)
DELETE FROM tokens
WHERE subscription_id IN (
    SELECT id FROM removed_subs
);
