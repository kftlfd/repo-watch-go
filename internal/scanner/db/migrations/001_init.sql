-- +goose Up
CREATE TABLE repositories (
    id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    latest_seen_tag TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT false,
    active_version INTEGER NOT NULL DEFAULT 0,
    last_checked_at TIMESTAMPTZ NOT NULL,
    leased_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE releases (
    id SERIAL PRIMARY KEY,
    repository_id TEXT NOT NULL REFERENCES repositories (id),
    tag TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repository_id, tag)
);

CREATE INDEX releases_created_at_idx ON releases (created_at);

CREATE TABLE outbox (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    leased_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    failed_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (created_at)
WHERE
    published_at IS NULL;

CREATE INDEX outbox_retryable_idx ON outbox (id)
WHERE
    published_at IS NULL
    AND failed_at IS NULL;

CREATE INDEX outbox_published_at_idx ON outbox (published_at)
WHERE
    published_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS outbox_published_at_idx;

DROP INDEX IF EXISTS outbox_retryable_idx;

DROP INDEX IF EXISTS outbox_unpublished_idx;

DROP INDEX IF EXISTS releases_created_at_idx;

DROP TABLE IF EXISTS outbox;

DROP TABLE IF EXISTS releases;

DROP TABLE IF EXISTS repositories;
