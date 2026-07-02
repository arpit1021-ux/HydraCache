# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /hydracache ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /hc ./cmd/cli

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

RUN adduser -D -g '' hydracache

WORKDIR /app

COPY --from=builder /hydracache .
COPY --from=builder /hc /usr/local/bin/hc

RUN mkdir -p /data/wal && chown -R hydracache:hydracache /data

USER hydracache

EXPOSE 7379 8379

ENTRYPOINT ["/app/hydracache"]
CMD ["-addr", ":7379", "-http", ":8379"]
