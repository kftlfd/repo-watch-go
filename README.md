# RepoWatch: GitHub release notifications

RepoWatch is a GitHub release notification service. Users subscribe to a repository using their email address and receive an email when a new release or tag is published.

## Stack

- Go
- [Gin](https://github.com/gin-gonic/gin)
- PostgreSQL, with [goose](https://github.com/pressly/goose) migrations and [sqlc](https://github.com/sqlc-dev/sqlc) queries
- RabbitMQ
- `log/slog` logging
- [Prometheus](https://prometheus.io/) metrics and [Grafana](https://grafana.com/) dashboards

## Configuration

- `cmd/<service>/config.go`: contains the supported and required environment variables, including defaults and validation
- `.env.example`: an example of the expected local environment

## Running

```bash
# create .env
cp .env.example .env
```

By default, the API and frontend are exposed on port `3000`.

API docs: [`http://localhost:3000/api/docs`]().

### Docker

```bash
# build and starts the whole stack
docker compose up --build
```

### Locally

See `go.mod` for the expected Go version.

```bash
# start the infrastructure (for example, in Docker)
docker compose up postgres rabbitmq

# start services, using a different PORT_ADMIN for each service
# DB migrations are applied when each service starts
PORT_ADMIN=4010 go run ./cmd/subscription
PORT_ADMIN=4020 go run ./cmd/scanner
PORT_ADMIN=4030 go run ./cmd/notification
PORT_ADMIN=4040 go run ./cmd/mailer
```

### Monitoring

```bash
docker compose -f monitoring/docker-compose.yml up -d
```

Grafana is exposed on port `3050`.

## Code generation and checks

See the [Makefile](./Makefile) for details.

```bash
make db-gen       # sqlc generate, then vet
make swagger      # regenerate API docs
make test         # unit tests
make test-all     # unit and integration tests
make check        # formatting, build, vet, lint, and tests
```

## Project overview

RepoWatch is an event-driven Go system organized into four services. Events flow from Subscription through Scanner and Notification to Mailer, carried by RabbitMQ.

1. **Subscription**
   - Provides a public web UI and JSON API for subscribing to repositories
   - Handles subscribe/unsubscribe flows with single-use random tokens
   - Implements double opt-in: sends a confirmation link that must be followed before notifications are enabled
   - Acts as the source of truth for subscriptions and watched repositories

2. **Scanner**
   - Maintains a projection of watched repositories
   - Periodically polls GitHub for new releases and tags in watched repositories
   - Broadcasts an event for each new release

3. **Notification**
   - Maintains a projection of subscribers
   - Consumes release events and fans them out into per-recipient email events

4. **Mailer**
   - Consumes email events and renders them into messages (currently logged to stdout only)

**Isolation.** Each service has its own database and no service reads another's data; there are no distributed transactions. Cross-service state propagates as events through RabbitMQ, so the system is eventually consistent. It targets at-least-once delivery and relies on unique database keys for idempotency.

**Transactional outbox.** State changes and outgoing events are written in a single database transaction. A dispatcher polls the outbox, leases rows, publishes with RabbitMQ publisher confirms, and records retries and failures. Most projections use upserts and version numbers that increase monotonically to reject stale events.

**Leases.** Outbox rows and the scanner's repository rows are leased with a timeout and `FOR UPDATE SKIP LOCKED`, allowing multiple instances to work concurrently without processing the same row twice.

**Messaging.** Each service consumes from its own queue. Messages that are rejected or cannot be routed go to the dead-letter exchange and queue; a periodic check records DLQ depth as a metric. RabbitMQ connections reconnect automatically.

**Admin servers.** Each service runs a dedicated admin server that reports health and readiness and exposes Prometheus metrics.

## Known limitations

- No real email delivery: `mailer` renders messages and logs them to stdout
- No authentication or authorization
- No HTTP rate limiting or abuse protection
- GitHub API polling instead of webhooks
- Eventual consistency: cross-service projections can be briefly stale, and no reconciliation procedure to repair drifts
- No DLQ replay tooling: DLQ depth is monitored, but messages are not replayed automatically
- Minimal security hardening: no CSP/HSTS/security headers, tokens stored in plaintext, and the API docs UI loaded from a CDN
