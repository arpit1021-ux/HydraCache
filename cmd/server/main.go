package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hydracache/hydracache/internal/auth"
	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/cluster"
	"github.com/hydracache/hydracache/internal/config"
	"github.com/hydracache/hydracache/internal/election"
	"github.com/hydracache/hydracache/internal/hashring"
	"github.com/hydracache/hydracache/internal/logging"
	"github.com/hydracache/hydracache/internal/metrics"
	"github.com/hydracache/hydracache/internal/network"
	"github.com/hydracache/hydracache/internal/persistence"
	"github.com/hydracache/hydracache/internal/pubsub"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	addr := flag.String("addr", ":7379", "TCP listen address")
	advertise := flag.String("advertise", "", "Address advertised to other nodes (defaults to -addr)")
	dataDir := flag.String("data-dir", "", "Directory for WAL and snapshot files (overrides config)")
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
	if *advertise != "" {
		cfg.Cluster.AdvertiseAddr = *advertise
	} else {
		cfg.Cluster.AdvertiseAddr = *addr
	}
	if *dataDir != "" {
		cfg.WAL.Dir = *dataDir
	}

	if *nodeID != "" {
		cfg.Cluster.NodeID = *nodeID
	}

	logger := logging.New(cfg.Log.Level, cfg.Log.Format, cfg.Log.Output)
	_ = logger

	if cfg.Cluster.NodeID == "" {
		cfg.Cluster.NodeID = generateShortID()
	}

	log.Printf("[hydracache] starting node %s on %s", shortIDStr(cfg.Cluster.NodeID), cfg.Server.Addr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- TLS setup (both must be ready before any dialing/listening) ---
	var serverTLSConfig *tls.Config
	if cfg.TLS.Enabled {
		material := network.TLSMaterial{
			CertFile:          cfg.TLS.CertFile,
			KeyFile:           cfg.TLS.KeyFile,
			CAFile:            cfg.TLS.CAFile,
			RequireClientCert: cfg.TLS.RequireClientCert,
		}
		var err error
		serverTLSConfig, err = network.BuildServerTLSConfig(material)
		if err != nil {
			log.Fatalf("[main] invalid config: %v", err)
		}
		clientTLSConfig, err := network.BuildClientTLSConfig(material)
		if err != nil {
			log.Fatalf("[main] invalid config: %v", err)
		}
		network.SetClientTLSConfig(clientTLSConfig)
		log.Printf("[main] TLS enabled (mutual=%v)", cfg.TLS.CAFile != "" && cfg.TLS.RequireClientCert)
	}

	evictionPolicy, policyErr := cache.EvictionPolicyFromString(cfg.Cache.EvictionPolicy)
	if policyErr != nil {
		log.Fatalf("[main] invalid config: %v", policyErr)
	}
	localCache := cache.New(&cache.Options{
		EvictionPolicy:       evictionPolicy,
		EvictionCapacity:     cfg.Cache.EvictionCapacity,
		MaxMemoryBytes:       cfg.Cache.MaxMemoryBytes,
		ActiveExpiration:     cfg.Cache.ActiveExpiration,
		ExpirationInterval:   cfg.Cache.ExpirationInterval,
		ExpirationSampleSize: cfg.Cache.ExpirationSampleSize,
	})

	// --- Persistence setup ---
	var wal *persistence.WAL
	var snapshotter *persistence.Snapshotter

	if cfg.WAL.Enabled {
		syncMode, err := persistence.SyncModeFromString(cfg.WAL.SyncMode)
		if err != nil {
			log.Fatalf("[main] invalid config: %v", err)
		}
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
	selfNode := cluster.NewNode(cfg.Cluster.NodeID, cfg.Cluster.AdvertiseAddr)

	hashRing := hashring.New(cfg.Cluster.VirtualNodes)
	hashRing.AddNode(cfg.Cluster.NodeID)

	clusterMgr := cluster.NewManager(selfNode, topo, hashRing, localCache)
	clusterMgr.SetDetectorThresholds(cfg.Cluster.PhiThreshold, cfg.Cluster.SuspectTimeout)
	if err := clusterMgr.Start(ctx); err != nil {
		log.Fatalf("Failed to start cluster manager: %v", err)
	}

	locator := hashring.NewLocator(hashRing, cfg.Cache.ReplicationFactor)
	_ = locator

	electionDir := filepath.Join(cfg.WAL.Dir, "election")
	termStore, err := election.NewFileTermStore(electionDir)
	if err != nil {
		log.Fatalf("Failed to init election term store: %v", err)
	}
	elect, err := election.New(election.Config{
		SelfID:    cfg.Cluster.NodeID,
		Store:     termStore,
		Transport: network.NewElectionTransport(),
		PeersFunc: func() []election.Peer {
			// Deliberately uses ALL known members (not just currently
			// reachable ones) so quorum is computed against total known
			// cluster size — a minority partition must never be able to
			// compute "quorum" against only the nodes it can still see.
			nodes := topo.AllNodes()
			peers := make([]election.Peer, 0, len(nodes))
			for _, n := range nodes {
				if n.ID == cfg.Cluster.NodeID || n.GetHealth() == cluster.HealthLeft {
					continue
				}
				peers = append(peers, election.Peer{ID: n.ID, Address: n.Address})
			}
			return peers
		},
		ElectionTimeoutMax: cfg.Cluster.ElectionTimeout,
		ElectionTimeoutMin: cfg.Cluster.ElectionTimeout / 2,
		HeartbeatInterval:  cfg.Cluster.HeartbeatInterval,
		LeaseTimeout:       cfg.Cluster.ElectionLeaseTimeout,
		RPCTimeout:         cfg.Cluster.ElectionRPCTimeout,
	})
	if err != nil {
		log.Fatalf("Failed to init election: %v", err)
	}
	elect.OnBecomeLeader(func() {
		log.Printf("[main] this node is now the cluster coordinator (term %d)", elect.Term())
		selfNode.SetRole(cluster.RoleLeader)
		topo.SetNodeRole(cfg.Cluster.NodeID, cluster.RoleLeader)
	})
	elect.OnLoseLeadership(func() {
		log.Printf("[main] this node lost cluster coordinator status")
		selfNode.SetRole(cluster.RolePeer)
		topo.SetNodeRole(cfg.Cluster.NodeID, cluster.RolePeer)
	})

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
			Addr:      cfg.Server.Addr,
			MaxConns:  cfg.Server.MaxConns,
			TLSConfig: serverTLSConfig,
		}, localCache, wal)
	} else {
		tcpServer = network.NewServer(network.ServerConfig{
			Addr:      cfg.Server.Addr,
			MaxConns:  cfg.Server.MaxConns,
			TLSConfig: serverTLSConfig,
		}, localCache)
	}

	tcpServer.SetGossip(clusterMgr.Gossip())
	tcpServer.SetReplication(cfg.Cluster.NodeID, clusterMgr.Registry(), locator)
	tcpServer.SetElection(elect)
	tcpServer.SetEpochSource(topo.Epoch)
	tcpServer.SetReplicationMode(cfg.Cache.ReplicationMode, cfg.Cache.ReplicationAckCount, cfg.Cache.ReplicationSyncTimeout)
	tcpServer.SetMetricsCollector(collector)
	tcpServer.SetMigrationChecker(clusterMgr)

	if cfg.Auth.Enabled {
		users := make([]auth.User, 0, len(cfg.Auth.Users))
		for _, u := range cfg.Auth.Users {
			users = append(users, auth.User{
				Username:    u.Username,
				Password:    u.Password,
				Commands:    u.Commands,
				KeyPatterns: u.KeyPatterns,
			})
		}
		acl, err := auth.New(users)
		if err != nil {
			log.Fatalf("[main] invalid config: %v", err)
		}
		tcpServer.SetAuth(acl)
		log.Printf("[main] AUTH/ACL enabled (%d user(s))", len(users))
	}

	clusterMgr.SetPromotionGate(cfg.Cache.MaxPromotionLag, cfg.Cache.PromotionGateTimeout)

	if err := tcpServer.Start(ctx); err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}
	log.Printf("[main] TCP server listening on %s", cfg.Server.Addr)

	elect.Start(ctx)

	// --- Bootstrap from seeds ---
	if *join != "" {
		seeds := strings.Split(*join, ",")
		if err := clusterMgr.Bootstrap(seeds); err != nil {
			log.Printf("[main] bootstrap error: %v", err)
		}
	}

	// --- HTTP API ---
	if cfg.HTTP.Enabled {
		mux := http.NewServeMux()
		mux.Handle("/metrics", collector.PrometheusHandler())
		mux.HandleFunc("/api/cluster", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			type nodeJSON struct {
				ID             string    `json:"id"`
				Address        string    `json:"address"`
				Role           string    `json:"role"`
				Health         string    `json:"health"`
				Region         string    `json:"region"`
				Version        string    `json:"version"`
				Load           float64   `json:"load"`
				MemoryMB       int64     `json:"memory_mb"`
				ReplicationLag int64     `json:"replication_lag"`
				LastSeen       time.Time `json:"last_seen"`
				JoinedAt       time.Time `json:"joined_at"`
				Epoch          uint64    `json:"epoch"`
				IsReplicaOf    string    `json:"is_replica_of,omitempty"`
			}

			allNodes := topo.AllNodes()
			registry := clusterMgr.Registry()

			nodes := make([]nodeJSON, 0, len(allNodes))
			for _, n := range allNodes {
				var lag int64
				if n.IsReplica() {
					primaryID := registry.FindPrimaryForReplica(n.ID)
					if primaryID != "" {
						if rs, ok := registry.GetReplicaSet(primaryID); ok {
							if info, ok := rs.GetReplica(n.ID); ok {
								lag = info.GetLagSeq()
							}
						}
					}
				} else if n.IsLeader() {
					if rs, ok := registry.GetReplicaSet(n.ID); ok {
						for _, v := range rs.LagInfo() {
							if v > lag {
								lag = v
							}
						}
					}
				}

				nodes = append(nodes, nodeJSON{
					ID:             n.ID,
					Address:        n.Address,
					Role:           n.GetRole().String(),
					Health:         n.GetHealth().String(),
					Region:         n.Region,
					Version:        n.Version,
					Load:           n.Load,
					MemoryMB:       n.MemoryMB,
					ReplicationLag: lag,
					LastSeen:       n.LastSeen,
					JoinedAt:       n.JoinedAt,
					Epoch:          n.Epoch,
					IsReplicaOf:    n.IsReplicaOf,
				})
			}

			resp := struct {
				Nodes     []nodeJSON `json:"nodes"`
				Epoch     uint64     `json:"epoch"`
				UpdatedAt time.Time  `json:"updated_at"`
			}{
				Nodes:     nodes,
				Epoch:     topo.Epoch(),
				UpdatedAt: time.Now(),
			}

			data, _ := json.Marshal(resp)
			_, _ = w.Write(data)
		})
		mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			stats := localCache.Stats()
			data := fmt.Sprintf(`{"keys":%d,"hits":%d,"misses":%d,"hit_rate":%.1f}`,
				stats.Keys, stats.Hits, stats.Misses, stats.HitRate*100)
			_, _ = w.Write([]byte(data))
		})
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		})

		// --- Dashboard static files ---
		dashboardDir := filepath.Join(filepath.Dir(os.Args[0]), "dashboard", "dist")
		if _, err := os.Stat(dashboardDir); err == nil {
			dashFS, err := fs.Sub(os.DirFS(dashboardDir), ".")
			if err == nil {
				spa := newSPAHandler(dashFS)
				mux.Handle("/", spa)
				log.Printf("[main] dashboard served from %s", dashboardDir)
			}
		} else {
			log.Printf("[main] dashboard not found at %s, skipping UI", dashboardDir)
		}

		go func() {
			log.Printf("[main] HTTP server listening on %s", cfg.HTTP.Addr)
			if err := http.ListenAndServe(cfg.HTTP.Addr, mux); err != nil {
				log.Printf("[main] HTTP server error: %v", err)
			}
		}()
	}

	// --- Periodic metrics refresh ---
	// Pulls already-accurate totals from the cache, server, and topology
	// into the Prometheus collector, instead of leaving keys_total,
	// memory_bytes, evictions_total, hits/misses, connected_connections,
	// and nodes_alive/nodes_total sitting at a permanent zero — the
	// collector fields existed and were exported on /metrics, but
	// nothing ever called their setters.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := localCache.Stats()
				collector.SetKeys(int64(stats.Keys))
				collector.SetMemory(stats.MemoryBytes)
				collector.SetHits(stats.Hits)
				collector.SetMisses(stats.Misses)
				collector.SetEvictions(stats.Evictions)
				collector.SetConns(tcpServer.ConnectionCount())
				collector.SetAliveNodes(int64(topo.AliveCount()))
				collector.SetTotalNodes(int64(topo.NodeCount()))
			}
		}
	}()

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

func shortIDStr(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// spaHandler serves static files from an embedded filesystem, falling back
// to index.html for any path that doesn't match a real file (SPA routing).
type spaHandler struct {
	fs   fs.FS
	file http.Handler
}

func newSPAHandler(fsys fs.FS) *spaHandler {
	return &spaHandler{fs: fsys, file: http.FileServer(http.FS(fsys))}
}

func (h *spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Try to open the requested path.
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}
	if _, err := fs.Stat(h.fs, path); err == nil {
		h.file.ServeHTTP(w, r)
		return
	}
	// File not found — serve index.html for SPA client-side routing.
	r.URL.Path = "/"
	h.file.ServeHTTP(w, r)
}
