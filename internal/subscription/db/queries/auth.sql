-- name: CreateUser :one
INSERT INTO users (email, password_hash)
VALUES ($1, $2)
ON CONFLICT (email) DO NOTHING
RETURNING *;

-- name: GetUserByEmail :one
SELECT *
FROM users
WHERE email = $1;

-- name: GetUserByEmailForUpdate :one
SELECT *
FROM users
WHERE email = $1
FOR UPDATE;

-- name: UpdateUnverifiedUserPasswordHash :one
UPDATE users
SET
    password_hash = $2,
    updated_at = now()
WHERE email = $1
    AND email_verified_at IS NULL
RETURNING *;

-- name: MarkUserEmailVerified :one
UPDATE users
SET
    email_verified_at = COALESCE(email_verified_at, now()),
    updated_at = now()
WHERE email = $1
RETURNING *;

-- name: CreateSession :one
INSERT INTO sessions (user_email, expires_at)
VALUES ($1, $2)
RETURNING *;

-- name: GetSessionByID :one
SELECT *
FROM sessions
WHERE id = $1;

-- name: GetSessionByIDForUpdate :one
SELECT *
FROM sessions
WHERE id = $1
FOR UPDATE;

-- name: TouchSession :one
UPDATE sessions
SET
    last_used_at = now(),
    expires_at = $2
WHERE id = $1
    AND revoked_at IS NULL
RETURNING *;

-- name: RevokeSession :execrows
UPDATE sessions
SET revoked_at = now()
WHERE id = $1
    AND revoked_at IS NULL;

-- name: CreateRefreshToken :one
INSERT INTO refresh_tokens (session_id, token_hash, expires_at)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetRefreshTokenByHash :one
SELECT *
FROM refresh_tokens
WHERE token_hash = $1;

-- name: GetRefreshTokenByHashForUpdate :one
SELECT *
FROM refresh_tokens
WHERE token_hash = $1
FOR UPDATE;

-- name: MarkRefreshTokenUsed :one
UPDATE refresh_tokens
SET used_at = now()
WHERE id = $1
    AND used_at IS NULL
RETURNING *;

-- name: RevokeRefreshTokensBySession :execrows
UPDATE refresh_tokens
SET revoked_at = now()
WHERE session_id = $1
    AND revoked_at IS NULL;

-- name: UpsertUserToken :one
INSERT INTO user_tokens (user_email, token_type, token_hash)
VALUES ($1, $2, $3)
ON CONFLICT (user_email, token_type) DO UPDATE
SET
    token_hash = EXCLUDED.token_hash,
    created_at = now()
RETURNING *;

-- name: GetUserToken :one
SELECT *
FROM user_tokens
WHERE user_email = $1
    AND token_type = $2;

-- name: FindUserTokenForUpdate :one
SELECT *
FROM user_tokens
WHERE token_type = $1
    AND token_hash = $2
FOR UPDATE;

-- name: DeleteUserToken :exec
DELETE FROM user_tokens
WHERE id = $1;

-- name: DeleteUserTokensByEmailAndType :exec
DELETE FROM user_tokens
WHERE user_email = $1
    AND token_type = $2;

-- name: DeleteExpiredUserTokens :execrows
DELETE FROM user_tokens
WHERE created_at < @cutoff::timestamptz;

-- name: DeleteExpiredSessions :execrows
DELETE FROM sessions
WHERE expires_at < @cutoff::timestamptz
    OR (
        revoked_at IS NOT NULL
        AND revoked_at < @cutoff::timestamptz
    );

-- name: DeleteExpiredRefreshTokens :execrows
DELETE FROM refresh_tokens
WHERE expires_at < @cutoff::timestamptz;

-- name: DeleteUnverifiedUsers :execrows
DELETE FROM users
WHERE email_verified_at IS NULL
    AND created_at < @cutoff::timestamptz;
