-- name: DeleteExpiredConfirmTokens :execrows
DELETE FROM tokens
WHERE token_type = 'confirm'
    AND created_at < @cutoff::timestamptz;

-- name: DeleteOrphanedUnsubTokens :execrows
DELETE FROM tokens AS t
USING subscriptions AS s
WHERE t.subscription_id = s.id
    AND t.token_type = 'unsub'
    AND t.created_at < @cutoff::timestamptz
    AND s.active = false;

-- name: DeletePublishedOutbox :execrows
DELETE FROM outbox
WHERE published_at IS NOT NULL
    AND published_at < @cutoff::timestamptz;

-- name: DeleteFailedOutbox :execrows
DELETE FROM outbox
WHERE failed_at IS NOT NULL
    AND failed_at < @cutoff::timestamptz;

-- name: DeleteProcessedEvents :execrows
DELETE FROM processed_events
WHERE processed_at < @cutoff::timestamptz;
