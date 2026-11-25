-- name: UpsertToken :one
INSERT INTO tokens
    (subscription_id, token_type, token_hash)
VALUES
    ($1, $2, $3)
ON CONFLICT (subscription_id, token_type) DO UPDATE
SET
    token_hash = EXCLUDED.token_hash,
    created_at = now()
RETURNING *;

-- name: GetToken :one
SELECT *
FROM tokens
WHERE subscription_id = $1
    AND token_type = $2;

-- name: FindToken :one
SELECT *
FROM tokens
WHERE token_type = $1
    AND token_hash = $2;

-- name: FindTokenForUpdate :one
SELECT *
FROM tokens
WHERE token_type = $1
    AND token_hash = $2
FOR UPDATE;

-- name: FindTokenWDate :one
SELECT *
FROM tokens
WHERE token_type = $1
    AND token_hash = $2
    AND created_at > $3;

-- name: FindTokenWDateForUpdate :one
SELECT *
FROM tokens
WHERE token_type = $1
    AND token_hash = $2
    AND created_at > $3
FOR UPDATE;

-- name: DeleteTokensBySubscription :exec
DELETE FROM tokens
WHERE subscription_id = $1;

-- name: DeleteTokensBySubscriptionAndType :exec
DELETE FROM tokens
WHERE subscription_id = $1
    AND token_type = $2;
