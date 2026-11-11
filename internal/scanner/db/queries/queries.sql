-- name: CreateOrUpdateRepo :exec
INSERT INTO repositories
    (id, owner, name, latest_seen_tag, active, active_version, last_checked_at)
VALUES
    ($1, $2, $3, $4, $5, $6, now())
ON CONFLICT (id) DO UPDATE
SET
    owner = EXCLUDED.owner,
    name = EXCLUDED.name,
    latest_seen_tag = CASE
        WHEN repositories.active = false
        THEN EXCLUDED.latest_seen_tag
        ELSE repositories.latest_seen_tag
    END,
    active = EXCLUDED.active,
    active_version = EXCLUDED.active_version,
    updated_at = now()
WHERE repositories.id = EXCLUDED.id
    AND repositories.active_version < EXCLUDED.active_version;

-- name: GetReposScanBatch :many
WITH candidates AS (
    SELECT id
    FROM repositories
    WHERE active = true
        AND (leased_until IS NULL OR leased_until < now())
    ORDER BY last_checked_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE repositories
SET leased_until = $2
FROM candidates
WHERE repositories.id = candidates.id
RETURNING repositories.*;

-- name: MarkRepoInactive :execrows
UPDATE repositories
SET
    active = false,
    updated_at = now()
WHERE id = $1
    AND active = true
    AND active_version = $2;

-- name: MarkRepoProcessed :exec
UPDATE repositories
SET
    latest_seen_tag = $2,
    last_checked_at = now()
WHERE id = $1;

-- name: CreateRelease :execrows
INSERT INTO releases
    (repository_id, tag)
VALUES
    ($1, $2)
ON CONFLICT (repository_id, tag) DO NOTHING;

-- name: CreateOutboxRow :one
INSERT INTO
    outbox (event_type, payload)
VALUES
    ($1, $2)
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
