-- name: DeleteExpiredConfirmTokens :execrows
DELETE FROM tokens
WHERE token_type = 'confirm'
    AND created_at < @cutoff::timestamptz;

-- name: DeletePublishedOutbox :execrows
DELETE FROM outbox
WHERE published_at IS NOT NULL
    AND published_at < @cutoff::timestamptz;
