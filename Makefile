SWAG := go tool swag

format:
	goimports -local repo-watch -w . && \
	gofmt -w .

test:
	go test ./...

test-all:
	go test -tags=integration ./...

swagger:
	$(SWAG) init \
		--dir ./internal/subscription/api \
		--generalInfo swagger.go \
		--output ./internal/subscription/api/docs \
		--outputTypes go
