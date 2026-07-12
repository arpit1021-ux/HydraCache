# Stage 1: Build dashboard
FROM node:20-alpine AS dashboard-builder

WORKDIR /app/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
RUN npm run build

# Stage 2: Build Go binaries
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /hydracache ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /hc ./cmd/cli

# Stage 3: Runtime
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

RUN adduser -D -g '' hydracache

WORKDIR /app

COPY --from=builder /hydracache .
COPY --from=builder /hc /usr/local/bin/hc
COPY --from=dashboard-builder /app/dashboard/dist /app/dashboard/dist

RUN mkdir -p /data/wal && chown -R hydracache:hydracache /data

USER hydracache

EXPOSE 7379 8379

ENTRYPOINT ["/app/hydracache"]
CMD ["-addr", ":7379", "-http", ":8379"]
