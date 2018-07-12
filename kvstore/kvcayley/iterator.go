package kvcayley

import (
	"bytes"

	json "github.com/pquerna/ffjson/ffjson"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"github.com/noypi/kv"
)

type Iterator struct {
	uid            uint64
	tags           graph.Tagger
	nextPrefix     []byte
	dir            quad.Direction
	open           bool
	iter           kv.KVIterator
	qs             *QuadStore
	originalPrefix string
	checkID        []byte
}

func NewIterator(prefix string, d quad.Direction, value graph.Value, qs *QuadStore) *Iterator {
	vb := value.([]byte)
	p := make([]byte, 0, 2+hashSize)
	p = append(p, []byte(prefix)...)
	p = append(p, []byte(vb[1:])...)

	var rdr, _ = qs.dbreader()
	iter := rdr.PrefixIterator0(p)

	it := Iterator{
		uid:            iterator.NextUID(),
		nextPrefix:     p,
		checkID:        vb,
		dir:            d,
		originalPrefix: prefix,
		iter:           iter,
		open:           true,
		qs:             qs,
	}

	return &it
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.iter.Reset0()
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Clone() graph.Iterator {
	out := NewIterator(it.originalPrefix, it.dir, it.checkID, it.qs)
	out.tags.CopyFrom(it)
	return out
}

func (it *Iterator) Close() error {
	if it.open {
		it.iter.Close()
		it.open = false
	}
	return nil
}

func (it *Iterator) isLiveValue(val []byte) bool {
	var entry IndexEntry
	json.Unmarshal(val, &entry)
	return len(entry.History)%2 != 0
}

func (it *Iterator) Err() error {
	return it.iter.Error()
}

func (it *Iterator) Result() graph.Value {
	return it.iter.Key()
}

func (it *Iterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func PositionOf(prefix []byte, d quad.Direction, qs *QuadStore) int {
	if bytes.Equal(prefix, []byte("sp")) {
		switch d {
		case quad.Subject:
			return 2
		case quad.Predicate:
			return hashSize + 2
		case quad.Object:
			return 2*hashSize + 2
		case quad.Label:
			return 3*hashSize + 2
		}
	}
	if bytes.Equal(prefix, []byte("po")) {
		switch d {
		case quad.Subject:
			return 2*hashSize + 2
		case quad.Predicate:
			return 2
		case quad.Object:
			return hashSize + 2
		case quad.Label:
			return 3*hashSize + 2
		}
	}
	if bytes.Equal(prefix, []byte("os")) {
		switch d {
		case quad.Subject:
			return hashSize + 2
		case quad.Predicate:
			return 2*hashSize + 2
		case quad.Object:
			return 2
		case quad.Label:
			return 3*hashSize + 2
		}
	}
	if bytes.Equal(prefix, []byte("cp")) {
		switch d {
		case quad.Subject:
			return 2*hashSize + 2
		case quad.Predicate:
			return hashSize + 2
		case quad.Object:
			return 3*hashSize + 2
		case quad.Label:
			return 2
		}
	}
	panic("unreachable")
}

func (it *Iterator) Contains(v graph.Value) bool {
	val := v.([]byte)
	if val[0] == 'z' {
		return false
	}
	offset := PositionOf(val[0:2], it.dir, it.qs)
	if bytes.HasPrefix(val[offset:], it.checkID[1:]) {
		// You may ask, why don't we check to see if it's a valid (not deleted) quad
		// again?
		//
		// We've already done that -- in order to get the graph.Value token in the
		// first place, we had to have done the check already; it came from a Next().
		//
		// However, if it ever starts coming from somewhere else, it'll be more
		// efficient to change the interface of the graph.Value for LevelDB to a
		// struct with a flag for isValid, to save another random read.
		return true
	}
	return false
}

func (it *Iterator) Size() (int64, bool) {
	return it.qs.SizeOf([]byte(it.checkID)), true
}

func (it *Iterator) Next() bool {
	it.iter.Next()
	return it.iter.Valid()
}

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Name:      it.qs.NameOf(it.checkID),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      size,
		Direction: it.dir,
	}
}

const NameIterator = "kvcayley-freeq"

var kvcayleyDBType graph.Type

func init() {
	kvcayleyDBType = graph.RegisterIterator(NameIterator)
}

func Type() graph.Type { return kvcayleyDBType }

func (it *Iterator) Type() graph.Type { return kvcayleyDBType }
func (it *Iterator) Sorted() bool     { return false }

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *Iterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
	}
}

var _ graph.Nexter = &Iterator{}
