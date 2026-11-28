package hashring

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"
)

const testVnodes = 150

func testKeys(n int) []uint64 {
	r := rand.New(rand.NewPCG(1, 2))
	keys := make([]uint64, n)
	for i := range keys {
		keys[i] = r.Uint64()
	}
	return keys
}

func nodeNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("node-%d", i)
	}
	return names
}

func newRing(names ...string) *Ring {
	r := New(testVnodes)
	for _, n := range names {
		r.Add(n)
	}
	return r
}

func owners(r *Ring, keys []uint64) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i], _ = r.Get(k)
	}
	return out
}

func TestNewPanicsOnBadVnodes(t *testing.T) {
	for _, v := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("New(%d) did not panic", v)
				}
			}()
			New(v)
		}()
	}
}

func TestEmptyRing(t *testing.T) {
	r := New(testVnodes)
	if n, ok := r.Get(42); ok || n != "" {
		t.Fatalf("Get on an empty ring = %q, %v", n, ok)
	}
	if n, ok := r.GetBounded(42, func(string) bool { return true }); ok || n != "" {
		t.Fatalf("GetBounded on an empty ring = %q, %v", n, ok)
	}
}

func TestSingleNode(t *testing.T) {
	r := newRing("only")
	for _, k := range testKeys(1000) {
		if n, ok := r.Get(k); !ok || n != "only" {
			t.Fatalf("Get(%d) = %q, %v", k, n, ok)
		}
	}
}

func TestKeysAroundTheWrap(t *testing.T) {
	r := newRing(nodeNames(3)...)
	for _, k := range []uint64{0, 1, math.MaxUint64 - 1, math.MaxUint64} {
		if _, ok := r.Get(k); !ok {
			t.Fatalf("Get(%d) found nobody", k)
		}
	}
}

func TestOrderOfAddDoesNotMatter(t *testing.T) {
	names := nodeNames(8)
	reversed := make([]string, len(names))
	for i, n := range names {
		reversed[len(names)-1-i] = n
	}
	keys := testKeys(10_000)
	a, b := owners(newRing(names...), keys), owners(newRing(reversed...), keys)
	for i := range keys {
		if a[i] != b[i] {
			t.Fatalf("key %d: %s vs %s", keys[i], a[i], b[i])
		}
	}
}

func TestAddTwiceChangesNothing(t *testing.T) {
	names := nodeNames(4)
	keys := testKeys(10_000)
	before := owners(newRing(names...), keys)

	r := newRing(names...)
	r.Add(names[1])
	r.Add(names[1])
	after := owners(r, keys)
	for i := range keys {
		if before[i] != after[i] {
			t.Fatalf("key %d moved from %s to %s", keys[i], before[i], after[i])
		}
	}
}

func TestRoughlyEvenSpread(t *testing.T) {
	const nodes, nKeys = 8, 100_000
	r := newRing(nodeNames(nodes)...)
	count := map[string]int{}
	for _, n := range owners(r, testKeys(nKeys)) {
		count[n]++
	}
	if len(count) != nodes {
		t.Fatalf("only %d of %d nodes got keys", len(count), nodes)
	}
	mean := float64(nKeys) / nodes
	for n, c := range count {
		if dev := math.Abs(float64(c)-mean) / mean; dev > 0.25 {
			t.Errorf("%s got %d keys, %.0f%% away from the mean %.0f", n, c, dev*100, mean)
		}
	}
}

func TestMoreVnodesSpreadBetter(t *testing.T) {
	spread := func(vnodes int) float64 {
		r := New(vnodes)
		for _, n := range nodeNames(8) {
			r.Add(n)
		}
		count := map[string]int{}
		for _, n := range owners(r, testKeys(100_000)) {
			count[n]++
		}
		maxC := 0
		for _, c := range count {
			maxC = max(maxC, c)
		}
		return float64(maxC) / (100_000.0 / 8)
	}
	few, many := spread(5), spread(500)
	if many >= few {
		t.Fatalf("max/avg with 500 vnodes (%.2f) is not better than with 5 (%.2f)", many, few)
	}
}

func TestAddMovesKeysOnlyToTheNewNode(t *testing.T) {
	names := nodeNames(8)
	keys := testKeys(100_000)
	r := newRing(names...)
	before := owners(r, keys)

	r.Add("newcomer")
	after := owners(r, keys)

	moved := 0
	for i := range keys {
		if before[i] == after[i] {
			continue
		}
		moved++
		if after[i] != "newcomer" {
			t.Fatalf("key %d moved from %s to %s, not to the new node", keys[i], before[i], after[i])
		}
	}
	// 1 of 9 nodes is new, so about 1/9 of the keys move
	if share := float64(moved) / float64(len(keys)); share < 0.08 || share > 0.15 {
		t.Fatalf("%.1f%% of keys moved, want about 11%%", share*100)
	}
}

func TestRemoveMovesOnlyItsOwnKeys(t *testing.T) {
	names := nodeNames(8)
	keys := testKeys(100_000)
	r := newRing(names...)
	before := owners(r, keys)

	r.Remove("node-3")
	after := owners(r, keys)

	for i := range keys {
		if before[i] != "node-3" && before[i] != after[i] {
			t.Fatalf("key %d was on %s and moved to %s though %s stayed", keys[i], before[i], after[i], before[i])
		}
		if after[i] == "node-3" {
			t.Fatalf("key %d still goes to the removed node", keys[i])
		}
	}
}

func TestRemoveThenAddRestoresTheMapping(t *testing.T) {
	names := nodeNames(8)
	keys := testKeys(50_000)
	r := newRing(names...)
	before := owners(r, keys)

	r.Remove("node-5")
	r.Add("node-5")
	after := owners(r, keys)
	for i := range keys {
		if before[i] != after[i] {
			t.Fatalf("key %d: %s before, %s after", keys[i], before[i], after[i])
		}
	}
}

func TestRemoveUnknownAndLast(t *testing.T) {
	r := newRing("a")
	r.Remove("nobody")
	if n, ok := r.Get(7); !ok || n != "a" {
		t.Fatalf("Get = %q, %v", n, ok)
	}
	r.Remove("a")
	if n, ok := r.Get(7); ok || n != "" {
		t.Fatalf("Get on a ring emptied by Remove = %q, %v", n, ok)
	}
}

func TestBoundedAcceptAllEqualsGet(t *testing.T) {
	r := newRing(nodeNames(8)...)
	for _, k := range testKeys(10_000) {
		want, _ := r.Get(k)
		got, ok := r.GetBounded(k, func(string) bool { return true })
		if !ok || got != want {
			t.Fatalf("key %d: GetBounded = %q, %v, Get = %q", k, got, ok, want)
		}
	}
}

func TestBoundedSkipsTheRefusedAndTakesTheNextOne(t *testing.T) {
	r := newRing(nodeNames(8)...)
	for _, k := range testKeys(10_000) {
		first, _ := r.Get(k)
		got, ok := r.GetBounded(k, func(n string) bool { return n != first })
		if !ok || got == first {
			t.Fatalf("key %d: refused %s, got %q, %v", k, first, got, ok)
		}
	}
}

func TestBoundedNeverReturnsARefusedNode(t *testing.T) {
	r := newRing(nodeNames(8)...)
	refused := map[string]bool{"node-1": true, "node-2": true, "node-6": true}
	for _, k := range testKeys(10_000) {
		got, ok := r.GetBounded(k, func(n string) bool { return !refused[n] })
		if !ok || refused[got] {
			t.Fatalf("key %d: got %q, %v", k, got, ok)
		}
	}
}

// the walk has to step over all points of a node, not stop after asking the same node again and again
func TestBoundedReachesTheOnlyAcceptingNode(t *testing.T) {
	r := newRing(nodeNames(8)...)
	for _, k := range testKeys(5_000) {
		got, ok := r.GetBounded(k, func(n string) bool { return n == "node-7" })
		if !ok || got != "node-7" {
			t.Fatalf("key %d: got %q, %v", k, got, ok)
		}
	}
}

func TestBoundedAsksEachNodeOnce(t *testing.T) {
	r := newRing(nodeNames(4)...)
	asked := map[string]int{}
	_, ok := r.GetBounded(12345, func(n string) bool {
		asked[n]++
		return false
	})
	if ok {
		t.Fatal("nobody accepts, yet GetBounded said yes")
	}
	if len(asked) != 4 {
		t.Fatalf("asked %d nodes, want 4", len(asked))
	}
	for n, c := range asked {
		if c != 1 {
			t.Errorf("%s was asked %d times", n, c)
		}
	}
}

// one hot key hits the load cap of its node and spills to the next ones, nobody goes over the cap
func TestBoundedLoadsSpreadAHotKey(t *testing.T) {
	const nodes, requests, eps = 4, 1000, 0.25
	r := newRing(nodeNames(nodes)...)
	load := map[string]int{}
	hot := uint64(0xDEADBEEF12345678)

	for total := 0; total < requests; total++ {
		cap := int(math.Ceil((1 + eps) * float64(total+1) / nodes))
		n, ok := r.GetBounded(hot, func(n string) bool { return load[n]+1 <= cap })
		if !ok {
			t.Fatalf("request %d: nobody below the cap %d, loads %v", total, cap, load)
		}
		load[n]++
	}

	finalCap := int(math.Ceil((1 + eps) * float64(requests) / nodes))
	for n, l := range load {
		if l > finalCap {
			t.Errorf("%s has %d requests, cap is %d", n, l, finalCap)
		}
	}
	if len(load) < 3 {
		t.Errorf("the hot key landed on %d nodes only: %v", len(load), load)
	}
}

func TestConcurrentUse(t *testing.T) {
	r := newRing(nodeNames(4)...)
	keys := testKeys(1000)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, k := range keys {
				r.Get(k)
				r.GetBounded(k, func(string) bool { return true })
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 200 {
			name := fmt.Sprintf("tmp-%d", i%3)
			r.Add(name)
			r.Remove(name)
		}
	}()
	wg.Wait()
}

func BenchmarkGet(b *testing.B) {
	r := newRing(nodeNames(16)...)
	keys := testKeys(1 << 16)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		r.Get(keys[i&(len(keys)-1)])
	}
}

func BenchmarkGetBounded(b *testing.B) {
	r := newRing(nodeNames(16)...)
	keys := testKeys(1 << 16)
	// the first node on the way says no, the second says yes: a typical spill
	b.Run("spill-one", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			k := keys[i&(len(keys)-1)]
			first, _ := r.Get(k)
			r.GetBounded(k, func(n string) bool { return n != first })
		}
	})
	// nobody accepts: the walk goes all the way round
	b.Run("nobody", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			r.GetBounded(keys[i&(len(keys)-1)], func(string) bool { return false })
		}
	})
}

func TestChain(t *testing.T) {
	a, b := []byte("block one"), []byte("block two")
	if Chain(1, a) != Chain(1, a) {
		t.Fatal("not deterministic")
	}
	if Chain(1, a) == Chain(2, a) {
		t.Fatal("the hash of what came before does not matter")
	}
	if Chain(1, a) == Chain(1, b) {
		t.Fatal("the data does not matter")
	}
	if Chain(Chain(0, a), b) == Chain(Chain(0, b), a) {
		t.Fatal("the order of blocks does not matter")
	}
}
