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

	var wal *persistence.WAL
	var snapshotter *persistence.Snapshotter
	if cfg.WAL.Enabled {
		var err error
		wal, err = persistence.NewWAL(cfg.WAL.Dir, cfg.WAL.MaxSize, persistence.SyncBatch)
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

	broker := pubsub.NewBroker()
	_ = broker

	collector := metrics.NewCollector()

	tcpServer := network.NewServer(network.ServerConfig{
		Addr:     cfg.Server.Addr,
		MaxConns: cfg.Server.MaxConns,
	}, localCache)

	if err := tcpServer.Start(ctx); err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}
	log.Printf("[main] TCP server listening on %s", cfg.Server.Addr)

	if cfg.HTTP.Enabled {
		mux := http.NewServeMux()
		mux.Handle("/metrics", collector.PrometheusHandler())
		mux.HandleFunc("/api/cluster", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			data, _ := topo.MarshalJSON()
			w.Write(data)
		})
		mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			stats := localCache.Stats()
			data := fmt.Sprintf(`{"keys":%d,"hits":%d,"misses":%d,"hit_rate":%.4f}`,
				stats.Keys, stats.Hits, stats.Misses, stats.HitRate)
			w.Write([]byte(data))
		})
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("[hydracache] shutting down...")

	cancel()
	detector.Stop()
	elect.Stop()
	tcpServer.Shutdown()
	if wal != nil {
		wal.Close()
	}
	if snapshotter != nil {
		snapshotter.Stop()
	}
	clusterMgr.Shutdown()
	localCache.Shutdown()

	log.Println("[hydracache] shutdown complete")
}

func generateShortID() string {
	return fmt.Sprintf("%08x", time.Now().UnixNano()%0xffffffff)
}
