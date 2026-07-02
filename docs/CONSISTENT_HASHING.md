# Consistent Hashing

## Problem

With naive hashing (`key % N`), adding or removing a node causes `K * (N-1)/N` keys to move — nearly the entire dataset.

## Solution

Consistent hashing maps both keys and nodes to positions on a ring (0 to 2^32). A key is owned by the first node found clockwise from its position.

## Virtual Nodes

Each physical node is mapped to V virtual positions on the ring. This ensures even distribution without depending on hash function quality.

```
Ring positions for Node A: hash("A#0"), hash("A#1"), ..., hash("A#149")
Ring positions for Node B: hash("B#0"), hash("B#1"), ..., hash("B#149")
```

## Key Lookup

1. Hash the key → position on ring
2. Binary search for first node position clockwise
3. That node owns the key

**Complexity**: O(log V) where V = virtual nodes per physical node.

## Node Addition

When a new node joins, only keys in the segment between the new node and its predecessor need to move. This is approximately K/N keys.

```
Before: Node A owns [0, 120]     Node B owns [120, 360]
After:  Node A owns [0, 120]     Node C owns [120, 200]     Node B owns [200, 360]
                                  (keys in [120, 200] moved from B to C)
```

## Node Removal

When a node leaves, its keys move to the next clockwise node. Only the affected segment moves.

## Implementation

- **Hash function**: FNV-1a (fast, good distribution)
- **Ring structure**: Sorted slice of uint32 positions + map to node IDs
- **Lookup**: Binary search (`sort.Search`)
- **Rebalancing**: Compute affected range, migrate only those keys
