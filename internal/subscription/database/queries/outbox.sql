-- name: CreateOutboxRow :one
INSERT INTO
    outbox (event_type, payload)
VALUES
    ($1, $2)
RETURNING *;

-- name: GetOutboxBatch :many
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
