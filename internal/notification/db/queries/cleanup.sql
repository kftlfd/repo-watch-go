-- name: DeletePublishedOutbox :execrows
DELETE FROM outbox
WHERE published_at IS NOT NULL
    AND published_at < @cutoff::timestamptz;
