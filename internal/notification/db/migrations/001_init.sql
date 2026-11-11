-- +goose Up
CREATE TABLE subscriptions (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    repository_id TEXT NOT NULL,
    unsub_token TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT false,
    active_version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (email, repository_id)
);

CREATE TABLE outbox (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    email TEXT NOT NULL,
    repository_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    unsub_token TEXT NOT NULL,
    payload JSONB NOT NULL,    
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    leased_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    failed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX outbox_dedupe_idx ON outbox (email, repository_id, tag)
WHERE
    failed_at IS NULL;

CREATE INDEX outbox_failed_at_idx ON outbox (failed_at)
WHERE
    failed_at IS NOT NULL;

CREATE INDEX outbox_retryable_idx ON outbox (id)
WHERE
    published_at IS NULL
    AND failed_at IS NULL;

CREATE INDEX outbox_published_at_idx ON outbox (published_at)
WHERE
    published_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS outbox_published_at_idx;

DROP INDEX IF EXISTS outbox_failed_at_idx;

DROP INDEX IF EXISTS outbox_dedupe_idx;

DROP INDEX IF EXISTS outbox_retryable_idx;

DROP TABLE IF EXISTS outbox;

DROP TABLE IF EXISTS subscriptions;
