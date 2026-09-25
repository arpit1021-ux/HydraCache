// Command readthrough is a reference application for HydraCache: a small
// product-catalog API that uses HydraCache as a cache-aside (read-through)
// layer in front of a real Postgres database, plus an optional live
// "kill a node" control for demonstrating that the app keeps serving
// correct data — just slower — while HydraCache is degraded.
//
// This is a demo, not a template for a production deployment. In
// particular, -enable-chaos-controls wires HTTP endpoints directly to
// `docker stop`/`docker start` against named containers; it defaults to
// off and must never be turned on outside a local demo environment.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("addr", ":9000", "HTTP listen address for the demo API")
	postgresDSN := flag.String("postgres-dsn", "postgres://postgres:postgres@localhost:5432/readthrough?sslmode=disable", "Postgres connection string")
	cacheAddr := flag.String("cache-addr", "localhost:7379", "HydraCache node address this demo connects to")
	cacheTTL := flag.Duration("cache-ttl", 30*time.Second, "TTL for cached products")
	simulateDBLatency := flag.Duration("simulate-db-latency", 80*time.Millisecond, "artificial delay added to every database query, standing in for realistic network/query latency a local demo Postgres doesn't otherwise have (see store.go)")
	enableChaosControls := flag.Bool("enable-chaos-controls", false, "expose /admin/nodes/{id}/kill and /revive, which run docker stop/start on HydraCache containers — demo only, never enable outside a local environment")
	chaosContainerPrefix := flag.String("chaos-container-prefix", "hydracache-node-", "container name prefix for chaos controls (container is prefix+node-id)")
	chaosNodeIDs := flag.String("chaos-node-ids", "1,2,3,4,5", "comma-separated node IDs chaos controls may act on")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	db, err := sql.Open("pgx", *postgresDSN)
	if err != nil {
		logger.Error("failed to open postgres connection", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := db.PingContext(ctx); err != nil {
		cancel()
		logger.Error("failed to reach postgres", "dsn", redactDSN(*postgresDSN), "error", err)
		os.Exit(1)
	}
	cancel()
	logger.Info("connected to postgres")

	redisClient := redis.NewClient(&redis.Options{
		Addr:        *cacheAddr,
		Protocol:    2, // HydraCache speaks RESP2 only — see COMMANDS.md.
		DialTimeout: 3 * time.Second,
	})
	defer redisClient.Close()

	store := NewPostgresStore(db, *simulateDBLatency)
	cache := NewProductCache(redisClient, *cacheTTL)
	service := NewService(store, cache)

	var chaos *ChaosController
	if *enableChaosControls {
		chaos = NewChaosController(*chaosContainerPrefix, splitCSV(*chaosNodeIDs))
		logger.Warn("chaos controls ENABLED — /admin/nodes/{id}/kill and /revive will run docker stop/start; this must never be enabled outside a local demo")
	}

	api := NewAPI(service, chaos, logger)

	server := &http.Server{
		Addr:              *addr,
		Handler:           withRequestLogging(logger, api.Routes()),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("readthrough demo listening", "addr", *addr, "cache_addr", *cacheAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// redactDSN avoids logging a password embedded in a postgres:// URL.
func redactDSN(dsn string) string {
	at := strings.Index(dsn, "@")
	scheme := strings.Index(dsn, "://")
	if at == -1 || scheme == -1 || at < scheme {
		return dsn
	}
	return dsn[:scheme+3] + "***@" + dsn[at+1:]
}

func withRequestLogging(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("request", "method", r.Method, "path", r.URL.Path, "duration_ms", fmt.Sprintf("%.1f", time.Since(start).Seconds()*1000))
	})
}
