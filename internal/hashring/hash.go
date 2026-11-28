package hashring

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// Hash maps data to a position on the ring. The result is the same in every process and every run,
// so several gateways agree on where a key lives
func Hash(data []byte) uint64 { return xxhash.Sum64(data) }

func HashString(s string) uint64 { return xxhash.Sum64String(s) }

// Chain hashes data together with prev, the hash of everything that came before it. The same block
// in a different place of a text gives a different result, so a chain identifies a whole prefix
func Chain(prev uint64, data []byte) uint64 {
	var d xxhash.Digest
	d.Reset()
	var p [8]byte
	binary.LittleEndian.PutUint64(p[:], prev)
	_, _ = d.Write(p[:])
	_, _ = d.Write(data)
	return d.Sum64()
}
