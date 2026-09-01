-- name: CreateRepository :one
INSERT INTO repositories 
    (id, owner, name, init_tag)
VALUES
    ($1, $2, $3, $4)
ON CONFLICT (id) DO NOTHING
RETURNING *;

-- name: GetRepositoryByID :one
SELECT *
FROM repositories
WHERE id = $1;

-- name: IncrementRepositoryActiveCount :one
UPDATE repositories
SET
    active_subscriptions_count = active_subscriptions_count + 1,
    active_version = CASE
        WHEN active_subscriptions_count = 0 THEN active_version + 1
        ELSE active_version
    END,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: DecrementRepositoryActiveCount :one
UPDATE repositories
SET
    active_subscriptions_count = CASE
        WHEN active_subscriptions_count <= 1 THEN 0
        ELSE active_subscriptions_count - 1
    END,
    active_version = CASE
        WHEN active_subscriptions_count <= 1 THEN active_version + 1
        ELSE active_version
    END,
    updated_at = now()
WHERE id = $1
RETURNING *;
