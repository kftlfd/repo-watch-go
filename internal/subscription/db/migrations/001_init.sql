-- +goose Up
CREATE TABLE repositories (
    id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    init_tag TEXT NOT NULL,
    active_subscriptions_count INTEGER NOT NULL DEFAULT 0,
    active_version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (owner, name)
);

CREATE TABLE subscriptions (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    repository_id TEXT NOT NULL REFERENCES repositories (id),
    active BOOLEAN NOT NULL DEFAULT false,
    active_version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (email, repository_id)
);

CREATE TABLE tokens (
    id SERIAL PRIMARY KEY,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions (id),
    token_type TEXT NOT NULL,
    token_val TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subscription_id, token_type)
);

CREATE INDEX tokens_confirm_created_at_idx ON tokens (created_at)
WHERE
    token_type = 'confirm';

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

DROP TABLE IF EXISTS outbox;

DROP INDEX IF EXISTS tokens_confirm_created_at_idx;

DROP TABLE IF EXISTS tokens;

DROP TABLE IF EXISTS subscriptions;

DROP TABLE IF EXISTS repositories;
