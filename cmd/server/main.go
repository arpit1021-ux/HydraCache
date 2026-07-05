package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/cluster"
	"github.com/hydracache/hydracache/internal/config"
	"github.com/hydracache/hydracache/internal/election"
	"github.com/hydracache/hydracache/internal/hashring"
	"github.com/hydracache/hydracache/internal/heartbeat"
	"github.com/hydracache/hydracache/internal/logging"
	"github.com/hydracache/hydracache/internal/metrics"
	"github.com/hydracache/hydracache/internal/network"
	"github.com/hydracache/hydracache/internal/persistence"
	"github.com/hydracache/hydracache/internal/pubsub"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	addr := flag.String("addr", ":7379", "TCP listen address")
	httpAddr := flag.String("http", ":8379", "HTTP API listen address")
	nodeID := flag.String("id", "", "Node ID (auto-generated if empty)")
	join := flag.String("join", "", "Address of existing node to join")
	flag.Parse()

	cfg := config.DefaultConfig()
	if *configPath != "" {
		var err error
		cfg, err = config.LoadConfig(*configPath)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
	}
	cfg.Server.Addr = *addr
	cfg.HTTP.Addr = *httpAddr

	if *nodeID != "" {
		cfg.Cluster.NodeID = *nodeID
	}

	logger := logging.New(cfg.Log.Level, cfg.Log.Format, cfg.Log.Output)
	_ = logger

	if cfg.Cluster.NodeID == "" {
		cfg.Cluster.NodeID = generateShortID()
	}

	log.Printf("[hydracache] starting node %s on %s", cfg.Cluster.NodeID[:8], cfg.Server.Addr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	localCache := cache.New(&cache.Options{
		EvictionPolicy:       cache.EvictionLRU,
		EvictionCapacity:     cfg.Cache.EvictionCapacity,
		ActiveExpiration:     cfg.Cache.ActiveExpiration,
		ExpirationInterval:   cfg.Cache.ExpirationInterval,
		ExpirationSampleSize: cfg.Cache.ExpirationSampleSize,
	})

	// --- Persistence setup ---
	var wal *persistence.WAL
	var snapshotter *persistence.Snapshotter

	if cfg.WAL.Enabled {
		var err error
		syncMode := persistence.SyncModeFromString(cfg.WAL.SyncMode)
		wal, err = persistence.NewWAL(cfg.WAL.Dir, cfg.WAL.MaxSize, syncMode)
		if err != nil {
			log.Printf("[main] warning: WAL init failed: %v", err)
		}
	}

	if cfg.WAL.EnabledSnapshot {
		var err error
		snapshotter, err = persistence.NewSnapshotter(cfg.WAL.Dir, cfg.WAL.SnapshotInterval, cfg.Cluster.NodeID)
		if err != nil {
			log.Printf("[main] warning: snapshot init failed: %v", err)
		}
	}

	// --- Startup recovery ---
	if wal != nil || snapshotter != nil {
		recoverer := persistence.NewRecoverer(wal, snapshotter)
		state, err := recoverer.Recover()
		if err != nil {
			log.Printf("[main] warning: recovery failed: %v", err)
		} else if state != nil && len(state.Entries) > 0 {
			entries := make(map[string]*cache.Entry, len(state.Entries))
			now := time.Now().UnixNano()
			for key, we := range state.Entries {
				// Convert WAL TTL (relative remaining nanoseconds) to
				// an absolute ExpiresAt. TTL==0 means no expiry.
				var expiresAt int64
				if we.TTL > 0 {
					expiresAt = now + we.TTL
				}
				entries[key] = cache.NewEntryWithTTL(we.Key, we.Value, expiresAt, we.Timestamp)
			}
			loaded := localCache.BulkLoad(entries)
			log.Printf("[main] recovered %d entries into cache (WAL seq=%d)", loaded, state.Seq)
		}
	}

	// --- Cluster, hash ring, etc. ---
	topo := cluster.NewTopology()
	selfNode := cluster.NewNode(cfg.Cluster.NodeID, cfg.Server.Addr)
	clusterMgr := cluster.NewManager(selfNode, topo)
	if err := clusterMgr.Start(ctx); err != nil {
		log.Fatalf("Failed to start cluster manager: %v", err)
	}

	hashRing := hashring.New(cfg.Cluster.VirtualNodes)
	hashRing.AddNode(cfg.Cluster.NodeID)

	locator := hashring.NewLocator(hashRing, cfg.Cache.ReplicationFactor)
	_ = locator

	detector := heartbeat.NewDetector(cfg.Cluster.NodeID)
	detector.StartChecking(cfg.Cluster.HeartbeatInterval)

	membership := heartbeat.NewMembership()
	membership.AddMember(cfg.Cluster.NodeID, cfg.Server.Addr)

	elect := election.New(cfg.Cluster.NodeID, 1)
	elect.OnBecomeLeader(func() {
		log.Printf("[main] this node is now the leader")
		selfNode.Role = cluster.RoleLeader
		topo.SetNodeRole(cfg.Cluster.NodeID, cluster.RoleLeader)
	})
	elect.Start()

	// --- Snapshot timer ---
	if snapshotter != nil && wal != nil {
		snapshotter.Start(func() persistence.SnapshotData {
			// Capture Seq BEFORE the snapshot so that any writes
			// arriving during or after the snapshot have Seq > captured,
			// ensuring they replay on recovery rather than being lost.
			seq := wal.Seq()
			snap := localCache.Snapshot()
			entries := make(map[string]persistence.SnapshotEntry, len(snap))
			for k, v := range snap {
				entries[k] = persistence.SnapshotEntry{
					Key:       v.Key,
					Value:     v.Value,
					ExpiresAt: v.ExpiresAt,
					CreatedAt: v.CreatedAt,
				}
			}
			return persistence.SnapshotData{
				Entries: entries,
				Seq:     seq,
			}
		})
		log.Printf("[main] snapshot timer started (interval=%v)", cfg.WAL.SnapshotInterval)
	}

	broker := pubsub.NewBroker()
	_ = broker

	collector := metrics.NewCollector()

	// --- TCP server ---
	var tcpServer *network.Server
	if wal != nil {
		tcpServer = network.NewServerWithWAL(network.ServerConfig{
			Addr:     cfg.Server.Addr,
			MaxConns: cfg.Server.MaxConns,
		}, localCache, wal)
	} else {
		tcpServer = network.NewServer(network.ServerConfig{
			Addr:     cfg.Server.Addr,
			MaxConns: cfg.Server.MaxConns,
		}, localCache)
	}

	if err := tcpServer.Start(ctx); err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}
	log.Printf("[main] TCP server listening on %s", cfg.Server.Addr)

	// --- HTTP API ---
	if cfg.HTTP.Enabled {
		mux := http.NewServeMux()
		mux.Handle("/metrics", collector.PrometheusHandler())
		mux.HandleFunc("/api/cluster", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			data, _ := topo.MarshalJSON()
			_, _ = w.Write(data)
		})
		mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			stats := localCache.Stats()
			data := fmt.Sprintf(`{"keys":%d,"hits":%d,"misses":%d,"hit_rate":%.4f}`,
				stats.Keys, stats.Hits, stats.Misses, stats.HitRate)
			_, _ = w.Write([]byte(data))
		})
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		})

		go func() {
			log.Printf("[main] HTTP server listening on %s", cfg.HTTP.Addr)
			if err := http.ListenAndServe(cfg.HTTP.Addr, mux); err != nil {
				log.Printf("[main] HTTP server error: %v", err)
			}
		}()
	}

	if *join != "" {
		log.Printf("[main] joining cluster via %s", *join)
	}

	// --- Wait for signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("[hydracache] shutting down...")

	// Shutdown order:
	// 1. Stop accepting new connections (cancel ctx + close server)
	// 2. Stop snapshot timer
	// 3. Final snapshot + truncate WAL
	// 4. Close WAL (sync + close file)
	// 5. Stop cluster components
	// 6. Stop cache background goroutines
	cancel()
	detector.Stop()
	elect.Stop()
	tcpServer.Shutdown()

	if snapshotter != nil {
		snapshotter.Stop()
	}

	// Final snapshot and WAL truncation — best-effort: if either fails,
	// we still proceed with shutdown to avoid blocking indefinitely.
	// A failed truncate means the next startup replays a few extra
	// (already-snapshotted) entries, which is idempotent.
	if snapshotter != nil && wal != nil {
		seq := wal.Seq()
		snap := localCache.Snapshot()
		entries := make(map[string]persistence.SnapshotEntry, len(snap))
		for k, v := range snap {
			entries[k] = persistence.SnapshotEntry{
				Key:       v.Key,
				Value:     v.Value,
				ExpiresAt: v.ExpiresAt,
				CreatedAt: v.CreatedAt,
			}
		}
		sd := persistence.SnapshotData{
			Entries: entries,
			Seq:     seq,
		}
		if err := snapshotter.Save(sd); err != nil {
			log.Printf("[main] warning: final snapshot failed: %v", err)
		} else if err := wal.Truncate(); err != nil {
			log.Printf("[main] warning: WAL truncate failed: %v", err)
		}
	}

	if wal != nil {
		if err := wal.Close(); err != nil {
			log.Printf("[main] warning: WAL close failed: %v", err)
		}
	}

	clusterMgr.Shutdown()
	localCache.Shutdown()

	log.Println("[hydracache] shutdown complete")
}

func generateShortID() string {
	return fmt.Sprintf("%08x", time.Now().UnixNano()%0xffffffff)
}
