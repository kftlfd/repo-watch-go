# syntax=docker/dockerfile:1

FROM golang:1.27.3-alpine AS build

ARG SERVICE
RUN test -n "$SERVICE"

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -buildvcs=false \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/service \
    "./cmd/${SERVICE}"

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 app \
    && adduser -S -D -H -u 10001 -G app app

COPY --from=build /out/service /usr/local/bin/service

USER app

ENTRYPOINT ["/usr/local/bin/service"]
