# Multi-stage build: compile a static binary, ship it on a tiny base image.
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download || true
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o /count-consumer .

FROM alpine:3.20 AS runtime
WORKDIR /app
COPY --from=builder /count-consumer ./count-consumer
EXPOSE 3001
CMD ["./count-consumer"]
