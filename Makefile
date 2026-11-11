GOOSE := go run github.com/pressly/goose/v3/cmd/goose@v3.29.0
SQLC := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
SWAG := go run github.com/swaggo/swag/cmd/swag@v1.16.6
GOIMPORTS := go run golang.org/x/tools/cmd/goimports@v0.52.0
GOVULNCHECK := go run golang.org/x/vuln/cmd/govulncheck@latest
GOLANGCILINT := GO_MOD_CACHE="$$(go env GOMODCACHE)"; \
	GO_BUILD_CACHE="$$(go env GOCACHE)"; \
	GO_LINT_CACHE="$$(echo ~/.cache/golangci-lint)"; \
	mkdir -p "$$GO_MOD_CACHE" "$$GO_BUILD_CACHE" "$$GO_LINT_CACHE"; \
	docker run --rm -t -v $$(pwd):/app -w /app --user $$(id -u):$$(id -g) \
	-v $$GO_MOD_CACHE:/tmp/cache/mod -e GOMODCACHE=/tmp/cache/mod \
	-v $$GO_BUILD_CACHE:/tmp/cache/go-build -e GOCACHE=/tmp/cache/go-build \
	-v $$GO_LINT_CACHE:/tmp/cache/lint -e GOLANGCI_LINT_CACHE=/tmp/cache/lint \
	golangci/golangci-lint:v2.15.0 golangci-lint

# =================

dev-infra-up:
	docker compose up -d postgres rabbitmq

dev-infra-down:
	docker compose down

db-gen:
	$(SQLC) generate
	$(SQLC) vet

swagger:
	$(SWAG) init \
		--dir ./internal/subscription/api \
		--generalInfo swagger.go \
		--output ./internal/subscription/api/docs \
		--outputTypes go

check: format-check build vet lint test

format-check:
	$(GOIMPORTS) -local repo-watch -d .
	gofmt -d .

format:
	$(GOIMPORTS) -local repo-watch -w .

build:
	go build ./...

vet:
	go vet ./...

lint:
	${GOLANGCILINT} run

test:
	go test ./...

test-all:
	go test -tags=integration ./...

test-all-race:
	go test -tags=integration -race ./...

vulncheck:
	${GOVULNCHECK} ./...
