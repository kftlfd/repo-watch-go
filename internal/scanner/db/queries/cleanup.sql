-- name: DeletePublishedOutbox :execrows
DELETE FROM outbox
WHERE published_at IS NOT NULL
    AND published_at < @cutoff::timestamptz;

-- name: DeleteOldReleases :execrows
DELETE FROM releases
WHERE created_at < @cutoff::timestamptz;
