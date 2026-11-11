-- name: CreateOutboxRow :one
INSERT INTO
    outbox (event_type, event_version, payload)
VALUES
    ($1, $2, $3)
RETURNING *;

-- name: GetEventTypeCount :one
SELECT count(*)
FROM outbox
WHERE event_type = $1;

-- name: GetOutboxBatch :many
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
