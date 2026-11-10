-- name: GetOutboxPendingStats :one
SELECT
    count(*) OVER () AS pending,
    created_at AS oldest_created_at
FROM outbox
WHERE published_at IS NULL
    AND failed_at IS NULL
ORDER BY created_at ASC
LIMIT 1;
