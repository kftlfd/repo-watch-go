-- +goose Up
CREATE TABLE repositories (
    id TEXT PRIMARY KEY,
    github_owner TEXT NOT NULL,
    github_name TEXT NOT NULL,
    active_subscription_count INTEGER NOT NULL DEFAULT 0,
    active_state_version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (github_owner, github_name)
);

CREATE TABLE subscriptions (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    repository_id TEXT NOT NULL REFERENCES repositories (id),
    active BOOLEAN NOT NULL DEFAULT false,
    active_state_version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (email, repository_id)
);

CREATE TABLE outbox (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    published_at TIMESTAMP
);

CREATE INDEX outbox_unpublished_idx ON outbox (created_at)
WHERE
    published_at IS NULL;

-- +goose Down
DROP TABLE repositories;

DROP TABLE subscriptions;

DROP TABLE outbox;

DROP INDEX outbox_unpublished_idx;
