-- name: UpsertSubscriptionToken :one
INSERT INTO subscription_tokens
    (subscription_id, token_type, token_hash)
VALUES
    ($1, $2, $3)
ON CONFLICT (subscription_id, token_type) DO UPDATE
SET
    token_hash = EXCLUDED.token_hash,
    created_at = now()
RETURNING *;

-- name: GetSubscriptionToken :one
SELECT *
FROM subscription_tokens
WHERE subscription_id = $1
    AND token_type = $2;

-- name: FindSubscriptionToken :one
SELECT *
FROM subscription_tokens
WHERE token_type = $1
    AND token_hash = $2;

-- name: FindSubscriptionTokenForUpdate :one
SELECT *
FROM subscription_tokens
WHERE token_type = $1
    AND token_hash = $2
FOR UPDATE;

-- name: FindSubscriptionTokenWDate :one
SELECT *
FROM subscription_tokens
WHERE token_type = $1
    AND token_hash = $2
    AND created_at > $3;

-- name: FindSubscriptionTokenWDateForUpdate :one
SELECT *
FROM subscription_tokens
WHERE token_type = $1
    AND token_hash = $2
    AND created_at > $3
FOR UPDATE;

-- name: DeleteSubscriptionTokensBySubID :exec
DELETE FROM subscription_tokens
WHERE subscription_id = $1;

-- name: DeleteSubscriptionTokensBySubIDAndType :exec
DELETE FROM subscription_tokens
WHERE subscription_id = $1
    AND token_type = $2;
