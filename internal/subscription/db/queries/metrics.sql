-- name: GetOutboxPendingStats :one
SELECT
    count(*) OVER () AS pending,
    created_at AS oldest_created_at
FROM outbox
WHERE published_at IS NULL
    AND failed_at IS NULL
ORDER BY created_at ASC
LIMIT 1;

-- name: GetOutboxFailedStats :one
SELECT
    count(*) OVER () AS failed,
    failed_at AS oldest_failed_at
FROM outbox
WHERE failed_at IS NOT NULL
ORDER BY failed_at ASC
LIMIT 1;
