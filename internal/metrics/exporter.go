package metrics

import (
	"fmt"
	"net/http"
	"strings"
)

func (c *Collector) PrometheusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		var sb strings.Builder

		sb.WriteString("# HELP hydracache_requests_total Total number of requests\n")
		sb.WriteString("# TYPE hydracache_requests_total counter\n")
		sb.WriteString(fmt.Sprintf("hydracache_requests_total %d\n", c.requestsTotal.Load()))

		sb.WriteString("# HELP hydracache_hits_total Total cache hits\n")
		sb.WriteString("# TYPE hydracache_hits_total counter\n")
		sb.WriteString(fmt.Sprintf("hydracache_hits_total %d\n", c.hitsTotal.Load()))

		sb.WriteString("# HELP hydracache_misses_total Total cache misses\n")
		sb.WriteString("# TYPE hydracache_misses_total counter\n")
		sb.WriteString(fmt.Sprintf("hydracache_misses_total %d\n", c.missesTotal.Load()))

		sb.WriteString("# HELP hydracache_keys_total Current number of keys\n")
		sb.WriteString("# TYPE hydracache_keys_total gauge\n")
		sb.WriteString(fmt.Sprintf("hydracache_keys_total %d\n", c.keysTotal.Load()))

		sb.WriteString("# HELP hydracache_memory_bytes Memory usage in bytes\n")
		sb.WriteString("# TYPE hydracache_memory_bytes gauge\n")
		sb.WriteString(fmt.Sprintf("hydracache_memory_bytes %d\n", c.memoryBytes.Load()))

		sb.WriteString("# HELP hydracache_connected_connections Current connections\n")
		sb.WriteString("# TYPE hydracache_connected_connections gauge\n")
		sb.WriteString(fmt.Sprintf("hydracache_connected_connections %d\n", c.connectedConns.Load()))

		sb.WriteString("# HELP hydracache_nodes_alive Number of alive nodes\n")
		sb.WriteString("# TYPE hydracache_nodes_alive gauge\n")
		sb.WriteString(fmt.Sprintf("hydracache_nodes_alive %d\n", c.nodesAlive.Load()))

		sb.WriteString("# HELP hydracache_nodes_total Total cluster nodes\n")
		sb.WriteString("# TYPE hydracache_nodes_total gauge\n")
		sb.WriteString(fmt.Sprintf("hydracache_nodes_total %d\n", c.nodesTotal.Load()))

		sb.WriteString("# HELP hydracache_evictions_total Total evictions\n")
		sb.WriteString("# TYPE hydracache_evictions_total counter\n")
		sb.WriteString(fmt.Sprintf("hydracache_evictions_total %d\n", c.evictionsTotal.Load()))

		c.replicationLags.Range(func(key, value interface{}) bool {
			nodeID := key.(string)
			lag := value.(int64)
			sb.WriteString(fmt.Sprintf("hydracache_replication_lag{node=\"%s\"} %d\n", nodeID, lag))
			return true
		})

		w.Write([]byte(sb.String()))
	})
}
