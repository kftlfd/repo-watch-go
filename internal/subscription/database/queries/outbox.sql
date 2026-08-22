-- name: CreateOutboxRow :one
INSERT INTO
    outbox (event_type, payload)
VALUES
    ($1, $2)
RETURNING *;
