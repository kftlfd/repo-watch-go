SQLC := go tool sqlc
GOIMPORTS := go tool goimports
SWAG := go tool swag

GOLANGCILINT_VERSION := v2.15.0

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

check: format-check build vet lint test-all

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
	GO_MOD_CACHE="$$(go env GOMODCACHE)"; \
	GO_BUILD_CACHE="$$(go env GOCACHE)"; \
	GO_LINT_CACHE="$$(echo ~/.cache/golangci-lint)"; \
	mkdir -p "$$GO_MOD_CACHE" "$$GO_BUILD_CACHE" "$$GO_LINT_CACHE"; \
	docker run --rm -t -v $$(pwd):/app -w /app --user $$(id -u):$$(id -g) \
	-v $$GO_MOD_CACHE:/tmp/cache/mod -e GOMODCACHE=/tmp/cache/mod \
	-v $$GO_BUILD_CACHE:/tmp/cache/go-build -e GOCACHE=/tmp/cache/go-build \
	-v $$GO_LINT_CACHE:/tmp/cache/lint -e GOLANGCI_LINT_CACHE=/tmp/cache/lint \
	golangci/golangci-lint:$(GOLANGCILINT_VERSION) golangci-lint run

test:
	go test ./...

test-all:
	go test -tags=integration ./...

test-all-race:
	go test -tags=integration -race ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
