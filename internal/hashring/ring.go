// Package hashring is a consistent hash ring with virtual nodes and a bounded-loads walk
package hashring

import (
	"cmp"
	"slices"
	"sort"
	"strconv"
	"sync"
)

// Ring places named nodes on a circle of 2^64 positions. Every node owns vnodes points on it,
// the point of node "a" number i is at HashString("a#i"). A key goes to the node whose point is
// the first one at or after the key, going clockwise and wrapping past the top back to 0.
//
// Keys are already hashed by the caller: Get uses the key as a position as it is.
//
// A Ring is safe for concurrent use.
type Ring struct {
	nodeNames     map[string]struct{}
	points        []point
	pointsPerNode int
	mu            sync.RWMutex
}

type point struct {
	name     string
	position uint64
}

// New makes an empty ring where every node gets vnodes points. vnodes < 1 is a programming error, New panics
func New(pointsPerNode int) *Ring {
	if pointsPerNode < 1 {
		panic("hashring: vnodes must be > 0")
	}
	return &Ring{
		pointsPerNode: pointsPerNode,
		points:        make([]point, 0),
		nodeNames:     map[string]struct{}{},
	}
}

// Add puts a node on the ring. Adding a node that is already there changes nothing.
// The result must not depend on the order of Add calls
// if name is empty -> panic
func (r *Ring) Add(name string) {
	if len(name) == 0 {
		panic("hashring: node name cant be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodeNames[name]; ok {
		return
	}
	r.nodeNames[name] = struct{}{}
	for i := 0; i < r.pointsPerNode; i++ {
		pointName := name + "#" + strconv.Itoa(i)
		hs := HashString(pointName)
		r.points = append(r.points, point{
			name:     name,
			position: hs,
		})
	}

	slices.SortFunc(r.points, func(a, b point) int {
		if c := cmp.Compare(a.position, b.position); c != 0 {
			return c
		}
		return cmp.Compare(a.name, b.name)
	})
}

// Remove takes a node off the ring. Removing an unknown node changes nothing
func (r *Ring) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodeNames[name]; ok {
		delete(r.nodeNames, name)
		r.points = slices.DeleteFunc(r.points, func(p point) bool {
			return p.name == name
		})
	}
}

// Get returns the node that owns key, false if the ring is empty
func (r *Ring) Get(key uint64) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return "", false
	}
	pI := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].position >= key
	})
	if pI == len(r.points) {
		return r.points[0].name, true
	}
	return r.points[pI].name, true
}

// GetBounded starts where Get would and walks clockwise over distinct nodes
// (a node with many points is asked once), returning the first node accept says yes to.
// The caller decides what yes means, for example "this node is below its load cap".
// It returns false when the ring is empty or nobody accepts
func (r *Ring) GetBounded(key uint64, accept func(node string) bool) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return "", false
	}
	pI := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].position >= key
	})
	if pI == len(r.points) {
		pI = 0
	}
	// a small ring needs no allocation: the buffer lives on the stack until it is outgrown
	var buf [16]string
	seen := buf[:0]
	for step := 0; step < len(r.points); step++ {
		i := (step + pI) % len(r.points)
		name := r.points[i].name
		seenIdx := slices.Index(seen, name)
		if seenIdx >= 0 {
			continue
		}
		seen = append(seen, name)
		if accept(name) {
			return name, true
		}
		if len(seen) == len(r.nodeNames) {
			return "", false
		}
	}
	return "", false
}
