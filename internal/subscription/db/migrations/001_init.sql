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
    email TEXT NOT NULL CHECK (email = lower(email)),
    repository_id TEXT NOT NULL REFERENCES repositories (id),
    active BOOLEAN NOT NULL DEFAULT false,
    active_version INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (email, repository_id)
);

CREATE INDEX subscriptions_active_repository_idx ON subscriptions (repository_id)
WHERE
    active = true;

CREATE TABLE users (
    email TEXT PRIMARY KEY CHECK (email = lower(email)),
    password_hash TEXT NOT NULL,
    email_verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE sessions (
    id BIGSERIAL PRIMARY KEY,
    user_email TEXT NOT NULL REFERENCES users (email) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX sessions_user_email_idx ON sessions (user_email);

CREATE INDEX sessions_expires_at_idx ON sessions (expires_at);

CREATE TABLE refresh_tokens (
    id BIGSERIAL PRIMARY KEY,
    session_id BIGINT NOT NULL REFERENCES sessions (id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX refresh_tokens_session_id_idx ON refresh_tokens (session_id);

CREATE INDEX refresh_tokens_expires_at_idx ON refresh_tokens (expires_at);

CREATE TABLE user_tokens (
    id BIGSERIAL PRIMARY KEY,
    user_email TEXT NOT NULL REFERENCES users (email) ON DELETE CASCADE,
    token_type TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_email, token_type)
);

CREATE INDEX user_tokens_created_at_idx ON user_tokens (created_at);

CREATE TABLE subscription_tokens (
    id SERIAL PRIMARY KEY,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions (id),
    token_type TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subscription_id, token_type)
);

CREATE INDEX subscription_tokens_confirm_created_at_idx ON subscription_tokens (created_at)
WHERE
    token_type = 'confirm';

CREATE INDEX subscription_tokens_unsub_created_at_idx ON subscription_tokens (created_at)
WHERE
    token_type = 'unsub';

CREATE TABLE processed_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX processed_events_processed_at_idx ON processed_events (processed_at);

CREATE TABLE outbox (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL DEFAULT 1,
    correlation_id TEXT NOT NULL DEFAULT '',
    trace_parent TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    leased_until TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    failed_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (created_at)
WHERE
    published_at IS NULL;

CREATE INDEX outbox_retryable_idx ON outbox (next_attempt_at ASC NULLS FIRST, id)
WHERE
    published_at IS NULL
    AND failed_at IS NULL;

CREATE INDEX outbox_published_at_idx ON outbox (published_at)
WHERE
    published_at IS NOT NULL;

CREATE INDEX outbox_failed_at_idx ON outbox (failed_at)
WHERE
    failed_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS outbox_failed_at_idx;

DROP INDEX IF EXISTS outbox_published_at_idx;

DROP INDEX IF EXISTS outbox_retryable_idx;

DROP INDEX IF EXISTS outbox_unpublished_idx;

DROP TABLE IF EXISTS outbox;

DROP INDEX IF EXISTS processed_events_processed_at_idx;

DROP TABLE IF EXISTS processed_events;

DROP INDEX IF EXISTS subscription_tokens_unsub_created_at_idx;

DROP INDEX IF EXISTS subscription_tokens_confirm_created_at_idx;

DROP TABLE IF EXISTS subscription_tokens;

DROP INDEX IF EXISTS user_tokens_created_at_idx;

DROP TABLE IF EXISTS user_tokens;

DROP INDEX IF EXISTS refresh_tokens_expires_at_idx;

DROP INDEX IF EXISTS refresh_tokens_session_id_idx;

DROP TABLE IF EXISTS refresh_tokens;

DROP INDEX IF EXISTS sessions_expires_at_idx;

DROP INDEX IF EXISTS sessions_user_email_idx;

DROP TABLE IF EXISTS sessions;

DROP TABLE IF EXISTS users;

DROP INDEX IF EXISTS subscriptions_active_repository_idx;

DROP TABLE IF EXISTS subscriptions;

DROP TABLE IF EXISTS repositories;
