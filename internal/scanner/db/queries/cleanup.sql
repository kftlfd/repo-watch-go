-- name: DeletePublishedOutbox :execrows
DELETE FROM outbox
WHERE published_at IS NOT NULL
    AND published_at < @cutoff::timestamptz;

-- name: DeleteFailedOutbox :execrows
DELETE FROM outbox
WHERE failed_at IS NOT NULL
    AND failed_at < @cutoff::timestamptz;

-- name: DeleteOldReleases :execrows
DELETE FROM releases
WHERE created_at < @cutoff::timestamptz;

-- name: DeleteProcessedEvents :execrows
DELETE FROM processed_events
WHERE processed_at < @cutoff::timestamptz;
