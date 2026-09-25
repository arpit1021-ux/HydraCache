package chaostest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Node maps a logical node ID to its Docker container and network endpoints.
type Node struct {
	ID            string
	ContainerName string
	TCPPort       string
	HTTPPort      string
	TCPAddr       string // host:port for RESP
	HTTPAddr      string // host:port for HTTP API
}

// ClusterInfo is the JSON response from /api/cluster.
type ClusterInfo struct {
	Nodes []ClusterNode `json:"nodes"`
	Epoch uint64        `json:"epoch"`
}

type ClusterNode struct {
	ID             string `json:"id"`
	Address        string `json:"address"`
	Role           string `json:"role"`
	Health         string `json:"health"`
	ReplicationLag int64  `json:"replication_lag"`
	IsReplicaOf    string `json:"is_replica_of,omitempty"`
}

// Harness manages the Docker-based cluster and provides polling helpers.
type Harness struct {
	Nodes       map[string]*Node
	ComposeDir  string // path to deploy/ directory
	NetworkName string
	startedByUs bool
}

var nodeRegistry = []*Node{
	{ID: "node-1", ContainerName: "hydracache-node-1", TCPPort: "7379", HTTPPort: "8379"},
	{ID: "node-2", ContainerName: "hydracache-node-2", TCPPort: "7380", HTTPPort: "8380"},
	{ID: "node-3", ContainerName: "hydracache-node-3", TCPPort: "7381", HTTPPort: "8381"},
	{ID: "node-4", ContainerName: "hydracache-node-4", TCPPort: "7382", HTTPPort: "8382"},
	{ID: "node-5", ContainerName: "hydracache-node-5", TCPPort: "7383", HTTPPort: "8383"},
}

func NewHarness(composeDir string) *Harness {
	h := &Harness{
		Nodes:      make(map[string]*Node),
		ComposeDir: composeDir,
	}
	for _, n := range nodeRegistry {
		node := *n
		node.TCPAddr = "localhost:" + n.TCPPort
		node.HTTPAddr = "localhost:" + n.HTTPPort
		h.Nodes[n.ID] = &node
	}
	return h
}

// Cleanup tears down the cluster if the harness started it.
func (h *Harness) Cleanup() {
	if !h.startedByUs {
		// Just restart any stopped containers.
		for _, n := range h.Nodes {
			_ = dockerExec("start", n.ContainerName)
		}
		return
	}
	fmt.Println("  Tearing down cluster...")
	_ = h.composeDown()
}

// ForceCleanup ensures the cluster is in a healthy state between scenarios.
// Does a full compose down/up cycle to guarantee clean state.
func (h *Harness) ForceCleanup() {
	// Reconnect any disconnected nodes first.
	for _, n := range h.Nodes {
		_ = h.ReconnectNode(n.ID)
	}
	// Full compose cycle: down (stop + remove) then up.
	_ = h.composeDown()
	CleanVolumes(h.ComposeDir)
	_ = h.composeUp()
	// Wait for all to be healthy. Best-effort: if this cleanup pass
	// doesn't converge in time, the next scenario's own precondition
	// check (every scenario calls WaitForAllHealthy itself before doing
	// anything) will fail with a clear message instead.
	_ = h.WaitForAllHealthy(90 * time.Second)
}

// FreshStart tears down any existing cluster, cleans volumes, and starts a fresh cluster.
func (h *Harness) FreshStart() {
	// Tear down existing cluster.
	_ = h.composeDown()
	// Clean volumes.
	CleanVolumes(h.ComposeDir)
	// Start fresh cluster.
	h.startedByUs = true
	if err := h.composeUp(); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: docker compose up failed: %v\n", err)
		return
	}
	// Wait for all nodes to become healthy.
	for _, n := range h.Nodes {
		if err := h.waitForHealth(n.HTTPAddr, 45*time.Second); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: node %s not healthy: %v\n", n.ID, err)
			continue
		}
		fmt.Printf("  %s healthy\n", n.ID)
	}
	h.detectNetworkName()
}

// WaitFor polls a condition with bounded timeout. Returns a descriptive error
// if the condition is never met — never uses fixed sleeps for synchronization.
func WaitFor(description string, timeout, interval time.Duration, condition func() (bool, string)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, state := condition()
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			if state == "" {
				state = "no state reported"
			}
			return fmt.Errorf("timeout after %s waiting for [%s]: last observed: %s", timeout, description, state)
		}
		time.Sleep(interval)
	}
}

// GetCluster fetches /api/cluster from the given node's HTTP API.
func (h *Harness) GetCluster(nodeID string) (*ClusterInfo, error) {
	n, ok := h.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node: %s", nodeID)
	}
	body, err := httpGet(n.HTTPAddr, "/api/cluster", 5*time.Second)
	if err != nil {
		return nil, err
	}
	var ci ClusterInfo
	if err := json.Unmarshal([]byte(body), &ci); err != nil {
		return nil, fmt.Errorf("failed to parse cluster response: %w", err)
	}
	return &ci, nil
}

// WaitForClusterHealth polls /api/cluster until the specified node shows the
// desired health state. Returns the full cluster info on success.
func (h *Harness) WaitForClusterHealth(targetNode, health string, timeout time.Duration) (*ClusterInfo, error) {
	var lastState string
	err := WaitFor(
		fmt.Sprintf("node %s health=%s", targetNode, health),
		timeout,
		500*time.Millisecond,
		func() (bool, string) {
			// Query from any alive surviving node.
			ci, err := h.getFirstAliveCluster()
			if err != nil {
				lastState = fmt.Sprintf("cluster query error: %v", err)
				return false, lastState
			}
			for _, n := range ci.Nodes {
				if n.ID == targetNode {
					lastState = fmt.Sprintf("node %s health=%s role=%s", n.ID, n.Health, n.Role)
					return n.Health == health, lastState
				}
			}
			lastState = fmt.Sprintf("node %s not found in cluster view", targetNode)
			return false, lastState
		},
	)
	if err != nil {
		return nil, err
	}
	return h.getFirstAliveCluster()
}

// WaitForNodeNotAlive polls until the specified node is either not present in
// the cluster view (removed after being detected dead) or has a non-alive health.
// Also checks that the node's HTTP endpoint is unreachable, as a secondary signal.
// This handles the case where a killed node is removed from topology entirely.
func (h *Harness) WaitForNodeNotAlive(targetNode string, timeout time.Duration) error {
	return WaitFor(
		fmt.Sprintf("node %s to be dead or removed", targetNode),
		timeout,
		1*time.Second,
		func() (bool, string) {
			// First check: is the node's HTTP endpoint unreachable?
			n := h.Nodes[targetNode]
			_, httpErr := httpGet(n.HTTPAddr, "/health", 2*time.Second)

			// Second check: cluster view.
			ci, err := h.getFirstAliveCluster()
			if err != nil {
				if httpErr != nil {
					return true, "" // unreachable AND can't query cluster → assume dead
				}
				return false, fmt.Sprintf("node HTTP unreachable but cluster query error: %v", err)
			}
			for _, cn := range ci.Nodes {
				if cn.ID == targetNode {
					if cn.Health != "alive" {
						return true, ""
					}
					// Node still alive in view — but if HTTP is unreachable AND
					// there's already a new leader, failover has occurred.
					if httpErr != nil {
						leaders := 0
						for _, other := range ci.Nodes {
							if other.Role == "leader" && other.ID != targetNode {
								leaders++
							}
						}
						if leaders > 0 {
							return true, ""
						}
					}
					return false, fmt.Sprintf("node %s health=%s (still alive, http reachable=%v)", cn.ID, cn.Health, httpErr == nil)
				}
			}
			// Node not found in cluster view — it was removed after being detected dead.
			return true, ""
		},
	)
}

// WaitForLeader polls until exactly one node in the cluster has role=leader.
func (h *Harness) WaitForLeader(timeout time.Duration) (string, error) {
	var leaderID string
	err := WaitFor(
		"a leader to be elected",
		timeout,
		500*time.Millisecond,
		func() (bool, string) {
			ci, err := h.getFirstAliveCluster()
			if err != nil {
				return false, fmt.Sprintf("cluster query error: %v", err)
			}
			leaders := 0
			for _, n := range ci.Nodes {
				if n.Role == "leader" {
					leaders++
					leaderID = n.ID
				}
			}
			if leaders == 1 {
				return true, ""
			}
			return false, fmt.Sprintf("%d leaders found among %d nodes", leaders, len(ci.Nodes))
		},
	)
	if err != nil {
		return "", err
	}
	return leaderID, nil
}

// WaitForAllHealthy polls until all nodes report health=alive.
func (h *Harness) WaitForAllHealthy(timeout time.Duration) error {
	return WaitFor(
		"all nodes healthy",
		timeout,
		500*time.Millisecond,
		func() (bool, string) {
			ci, err := h.getFirstAliveCluster()
			if err != nil {
				return false, fmt.Sprintf("cluster query error: %v", err)
			}
			alive := 0
			var notAlive []string
			for _, n := range ci.Nodes {
				if n.Health == "alive" {
					alive++
				} else {
					notAlive = append(notAlive, fmt.Sprintf("%s=%s", n.ID, n.Health))
				}
			}
			if alive == len(h.Nodes) {
				return true, ""
			}
			return false, fmt.Sprintf("%d/%d alive; not alive: [%s]", alive, len(h.Nodes), strings.Join(notAlive, ", "))
		},
	)
}

// StopNode stops a Docker container and verifies it is actually stopped.
func (h *Harness) StopNode(nodeID string) error {
	n := h.Nodes[nodeID]
	fmt.Printf("    Stopping %s...\n", n.ContainerName)
	out, err := exec.Command("docker", "stop", n.ContainerName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker stop %s failed: %w (output: %s)", n.ContainerName, err, string(out))
	}
	// Verify the container is actually stopped.
	time.Sleep(1 * time.Second)
	state, _ := h.ContainerState(n.ContainerName)
	if state == "running" {
		// Force kill if still running.
		_ = exec.Command("docker", "kill", n.ContainerName).Run()
		time.Sleep(1 * time.Second)
	}
	return nil
}

// ContainerState returns the running state of a container ("running", "exited", "paused", etc.).
func (h *Harness) ContainerState(containerName string) (string, error) {
	out, err := exec.Command("docker", "inspect", "--format", "{{.State.Status}}", containerName).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// StartNode starts a Docker container.
func (h *Harness) StartNode(nodeID string) error {
	n := h.Nodes[nodeID]
	fmt.Printf("    Starting %s...\n", n.ContainerName)
	out, err := exec.Command("docker", "start", n.ContainerName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker start %s failed: %w (output: %s)", n.ContainerName, err, string(out))
	}
	return nil
}

// RestartNode restarts a Docker container.
func (h *Harness) RestartNode(nodeID string) error {
	n := h.Nodes[nodeID]
	out, err := exec.Command("docker", "restart", n.ContainerName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker restart %s failed: %w (output: %s)", n.ContainerName, err, string(out))
	}
	return nil
}

// StartFreshNode starts a brand-new container that was never part of the cluster.
// It uses the same image as the existing nodes but with a fresh data volume.
// Returns the container name.
func (h *Harness) StartFreshNode(nodeID, seedAddr string) (string, error) {
	containerName := "hydracache-" + nodeID
	imageName := "deploy-cache-node-1" // reuse the same image

	// Remove any existing container with this name.
	_ = exec.Command("docker", "rm", "-f", containerName).Run()

	args := []string{
		"run", "-d",
		"--network", h.NetworkName,
		"--name", containerName,
		"-p", "7384:7379",
		"-p", "8384:8379",
		imageName,
		"-addr", ":7379",
		"-http", ":8379",
		"-id", nodeID,
		"-advertise", containerName + ":7379",
		"-data-dir", "/data/wal",
		"-join", seedAddr,
	}
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker run %s failed: %w (output: %s)", containerName, err, string(out))
	}
	return containerName, nil
}

// StopFreshNode stops and removes a dynamically-created container.
func (h *Harness) StopFreshNode(containerName string) {
	_ = exec.Command("docker", "rm", "-f", containerName).Run()
}

// DisconnectNode removes a container from the Docker bridge network.
func (h *Harness) DisconnectNode(nodeID string) error {
	n := h.Nodes[nodeID]
	if h.NetworkName == "" {
		return fmt.Errorf("network name not detected")
	}
	out, err := exec.Command("docker", "network", "disconnect", h.NetworkName, n.ContainerName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker network disconnect %s from %s failed: %w (output: %s)", n.ContainerName, h.NetworkName, err, string(out))
	}
	return nil
}

// ReconnectNode adds a container back to the Docker bridge network.
func (h *Harness) ReconnectNode(nodeID string) error {
	n := h.Nodes[nodeID]
	if h.NetworkName == "" {
		return fmt.Errorf("network name not detected")
	}
	out, err := exec.Command("docker", "network", "connect", h.NetworkName, n.ContainerName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker network connect %s to %s failed: %w (output: %s)", n.ContainerName, h.NetworkName, err, string(out))
	}
	return nil
}

func (h *Harness) detectNetworkName() {
	// Inspect the first container to find its bridge network.
	out, err := exec.Command("docker", "inspect", "--format",
		`{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}`,
		"hydracache-node-1").Output()
	if err != nil {
		h.NetworkName = "deploy_hydracache" // fallback
		return
	}
	name := strings.TrimSpace(string(out))
	if name != "" {
		h.NetworkName = name
	} else {
		h.NetworkName = "deploy_hydracache"
	}
}

func (h *Harness) getFirstAliveCluster() (*ClusterInfo, error) {
	// Try each node; the first one that responds is used.
	for _, n := range h.Nodes {
		ci, err := h.GetCluster(n.ID)
		if err == nil && len(ci.Nodes) > 0 {
			return ci, nil
		}
	}
	return nil, fmt.Errorf("no node reachable")
}

func (h *Harness) waitForHealth(httpAddr string, timeout time.Duration) error {
	return WaitFor(
		fmt.Sprintf("health check at %s", httpAddr),
		timeout,
		1*time.Second,
		func() (bool, string) {
			resp, err := httpGet(httpAddr, "/health", 2*time.Second)
			if err != nil {
				return false, fmt.Sprintf("error: %v", err)
			}
			if resp == "OK" {
				return true, ""
			}
			return false, fmt.Sprintf("response: %s", resp)
		},
	)
}

func (h *Harness) composeUp() error {
	out, err := exec.Command("docker", "compose", "-f", h.ComposeDir+"/docker-compose.yml", "up", "-d").CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w\noutput: %s", err, string(out))
	}
	return nil
}

func (h *Harness) composeDown() error {
	out, err := exec.Command("docker", "compose", "-f", h.ComposeDir+"/docker-compose.yml", "down", "-v").CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w\noutput: %s", err, string(out))
	}
	return nil
}

func dockerExec(args ...string) error {
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s failed: %w (output: %s)", strings.Join(args, " "), err, string(out))
	}
	return nil
}

func httpGet(addr, path string, timeout time.Duration) (string, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get("http://" + addr + path)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(body)), nil
}

// CleanVolumes removes the named Docker volumes used by the cluster.
func CleanVolumes(composeDir string) {
	volumes := []string{
		"deploy_node1-data", "deploy_node2-data", "deploy_node3-data",
		"deploy_node4-data", "deploy_node5-data",
	}
	for _, v := range volumes {
		_ = exec.Command("docker", "volume", "rm", "-f", v).Run()
	}
}
