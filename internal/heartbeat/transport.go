package heartbeat

import (
	"log"
	"sync"
	"time"
)

// PingFunc is called to send a PING to a peer. Returns the round-trip
// duration or an error if the peer is unreachable.
type PingFunc func(peerID, peerAddr string) (time.Duration, error)

// Transport sends periodic PING messages to peers and feeds the
// round-trip time into a heartbeat Detector for phi-accrual failure
// detection.
type Transport struct {
	selfID   string
	detector *Detector
	pingFn   PingFunc
	peersFn  func() []Peer
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// Peer is a minimal description of a cluster peer for transport purposes.
type Peer struct {
	ID      string
	Address string
}

func NewTransport(selfID string, detector *Detector, pingFn PingFunc, peersFn func() []Peer, interval time.Duration) *Transport {
	return &Transport{
		selfID:   selfID,
		detector: detector,
		pingFn:   pingFn,
		peersFn:  peersFn,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

func (t *Transport) Start() {
	t.wg.Add(1)
	go t.pingLoop()
}

func (t *Transport) Stop() {
	close(t.stopCh)
	t.wg.Wait()
}

func (t *Transport) pingLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.pingAllPeers()
		}
	}
}

func (t *Transport) pingAllPeers() {
	peers := t.peersFn()
	for _, peer := range peers {
		if peer.ID == t.selfID {
			continue
		}
		_, err := t.pingFn(peer.ID, peer.Address)
		if err != nil {
			log.Printf("[heartbeat] ping to %s failed: %v", shortID2(peer.ID), err)
			continue
		}
		t.detector.RecordHeartbeat(HeartbeatMessage{
			NodeID:    peer.ID,
			Timestamp: time.Now(),
		})
	}
}
