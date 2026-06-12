# Multi-stage build: compile a static binary, ship it on a tiny base image.
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
# Resolve and pin the module graph before copying sources for better caching.
RUN go mod download || true
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o /ingest-api .

FROM alpine:3.20 AS runtime
WORKDIR /app
COPY --from=builder /ingest-api ./ingest-api
EXPOSE 3000
CMD ["./ingest-api"]
