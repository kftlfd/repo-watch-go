SWAG := go tool swag

format:
	goimports -local repo-watch -w . && \
	gofmt -w .

swagger:
	$(SWAG) init \
		--dir ./internal/subscription/api \
		--generalInfo swagger.go \
		--output ./internal/subscription/api/docs \
		--outputTypes go
