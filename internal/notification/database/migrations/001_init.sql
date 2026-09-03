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
    email TEXT NOT NULL,
    repository_id TEXT NOT NULL,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    tag TEXT NOT NULL,
    unsub_token TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    leased_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    UNIQUE(email, repository_id, tag)
);

-- +goose Down
DROP TABLE IF EXISTS outbox;

DROP TABLE IF EXISTS subscriptions;
