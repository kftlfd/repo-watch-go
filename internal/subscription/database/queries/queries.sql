-- name: CreateRepository :one
INSERT INTO
    repositories (id, github_owner, github_name)
VALUES
    ($1, $2, $3) RETURNING *;

-- name: CreateSubscription :one
INSERT INTO
    subscriptions (email, repository_id)
VALUES
    ($1, $2) RETURNING *;

-- name: CreateOutboxRow :one
INSERT INTO
    outbox (event_type, payload)
VALUES
    ($1, $2) RETURNING *;
