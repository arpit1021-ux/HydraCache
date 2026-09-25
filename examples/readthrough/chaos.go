package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// ChaosController runs docker stop/start against the demo's own HydraCache
// containers, giving the demo's "kill a node" button something real to
// do — the same two commands internal/chaostest's harness already uses
// against the identical container names, not new speculative shell-out
// logic. It is only ever constructed when main.go's -enable-chaos-controls
// flag is set; nil means the /admin endpoints stay disabled. There is no
// path from a client request to this code that doesn't go through that
// explicit, off-by-default flag.
type ChaosController struct {
	containerPrefix string // e.g. "hydracache-node-" — container is prefix+id
	allowedNodeIDs  map[string]bool
}

func NewChaosController(containerPrefix string, nodeIDs []string) *ChaosController {
	allowed := make(map[string]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		allowed[id] = true
	}
	return &ChaosController{containerPrefix: containerPrefix, allowedNodeIDs: allowed}
}

func (c *ChaosController) containerName(nodeID string) (string, error) {
	if !c.allowedNodeIDs[nodeID] {
		return "", fmt.Errorf("unknown node id %q", nodeID)
	}
	return c.containerPrefix + nodeID, nil
}

func (c *ChaosController) Kill(ctx context.Context, nodeID string) error {
	container, err := c.containerName(nodeID)
	if err != nil {
		return err
	}
	out, err := exec.CommandContext(ctx, "docker", "stop", container).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker stop %s: %w (output: %s)", container, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (c *ChaosController) Revive(ctx context.Context, nodeID string) error {
	container, err := c.containerName(nodeID)
	if err != nil {
		return err
	}
	out, err := exec.CommandContext(ctx, "docker", "start", container).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker start %s: %w (output: %s)", container, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// NodeStatus reports one node's container state for the demo UI.
type NodeStatus struct {
	NodeID string `json:"node_id"`
	State  string `json:"state"` // "running", "exited", "unknown", ...
}

// Status queries docker inspect for every known node. It never fails the
// whole call over one unreachable container — a node genuinely being
// killed is the expected state this exists to show, not an error.
func (c *ChaosController) Status(ctx context.Context) []NodeStatus {
	statuses := make([]NodeStatus, 0, len(c.allowedNodeIDs))
	for nodeID := range c.allowedNodeIDs {
		container := c.containerPrefix + nodeID
		out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Status}}", container).Output()
		state := "unknown"
		if err == nil {
			state = strings.TrimSpace(string(out))
		}
		statuses = append(statuses, NodeStatus{NodeID: nodeID, State: state})
	}
	return statuses
}
