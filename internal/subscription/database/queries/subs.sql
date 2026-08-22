-- name: CreateSubscription :one
INSERT INTO subscriptions
    (email, repository_id)
VALUES
    ($1, $2)
ON CONFLICT (email, repository_id) DO NOTHING
RETURNING *;

-- name: GetSubscriptionByID :one
SELECT *
FROM subscriptions
WHERE id = $1;

-- name: ActiveSubscriptionExists :one
SELECT EXISTS (
    SELECT *
    FROM subscriptions
    WHERE email = $1
        AND repository_id = $2
        AND active = true
);

-- name: ActivateSubscription :one
UPDATE subscriptions
SET
    active = true,
    version = CASE
        WHEN active = false THEN version + 1
        ELSE version
    END,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: DeactivateSubscription :one
UPDATE subscriptions
SET
    active = false,
    version = CASE
        WHEN active = true THEN version + 1
        ELSE version
    END,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: ListSubscriptionsByEmail :many
SELECT
    repositories.id
FROM subscriptions
    JOIN repositories ON subscriptions.repository_id = repositories.id
WHERE subscriptions.email = $1
    AND subscriptions.active = true
ORDER BY subscriptions.created_at ASC;
