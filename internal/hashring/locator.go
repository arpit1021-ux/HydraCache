package hashring

type Locator struct {
	ring         *HashRing
	replicationFactor int
}

func NewLocator(ring *HashRing, replicationFactor int) *Locator {
	if replicationFactor <= 0 {
		replicationFactor = 2
	}
	return &Locator{
		ring:              ring,
		replicationFactor: replicationFactor,
	}
}

func (l *Locator) LocateKey(key string) []string {
	return l.ring.GetNodes(key, l.replicationFactor)
}

func (l *Locator) PrimaryNode(key string) string {
	return l.ring.GetNode(key)
}

func (l *Locator) ReplicaNodes(key string) []string {
	nodes := l.ring.GetNodes(key, l.replicationFactor)
	if len(nodes) <= 1 {
		return nil
	}
	return nodes[1:]
}

func (l *Locator) SetReplicationFactor(rf int) {
	l.replicationFactor = rf
}

func (l *Locator) ReplicationFactor() int {
	return l.replicationFactor
}
