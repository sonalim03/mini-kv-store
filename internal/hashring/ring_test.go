package hashring

import "testing"

func TestGetNode_ConsistentForSameKey(t *testing.T) {
	r := New()
	r.AddNode("node-a")
	r.AddNode("node-b")
	r.AddNode("node-c")

	n1, _ := r.GetNode("user:1001")
	n2, _ := r.GetNode("user:1001")
	if n1 != n2 {
		t.Fatalf("same key mapped to different nodes: %s vs %s", n1, n2)
	}
}

func TestAddNode_MinimalRemapping(t *testing.T) {
	r := New()
	r.AddNode("node-a")
	r.AddNode("node-b")

	keys := make([]string, 1000)
	before := make(map[string]string)
	for i := range keys {
		keys[i] = "key" + string(rune(i))
		n, _ := r.GetNode(keys[i])
		before[keys[i]] = n
	}

	r.AddNode("node-c")

	moved := 0
	for _, k := range keys {
		n, _ := r.GetNode(k)
		if n != before[k] {
			moved++
		}
	}

	// With consistent hashing, adding a 3rd node to 2 should remap roughly
	// 1/3 of keys, not anywhere near all of them. Assert it stays well
	// under a naive-modulo-hashing level of churn (~100%).
	if moved > 600 {
		t.Fatalf("too many keys remapped on node add: %d/1000 (expected well under with consistent hashing)", moved)
	}
	t.Logf("keys remapped after adding 3rd node: %d/1000", moved)
}

func TestReplicaNodes_Distinct(t *testing.T) {
	r := New()
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	replicas := r.GetReplicaNodes("some-key", 3)
	if len(replicas) != 3 {
		t.Fatalf("expected 3 distinct replicas, got %v", replicas)
	}
	seen := map[string]bool{}
	for _, n := range replicas {
		if seen[n] {
			t.Fatalf("duplicate node in replica set: %v", replicas)
		}
		seen[n] = true
	}
}
