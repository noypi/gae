package kvcayley

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"

	"github.com/noypi/kv"
)

type AllIterator struct {
	uid    uint64
	tags   graph.Tagger
	iter   kv.KVIterator
	dir    quad.Direction
	prefix []byte
	qs     *QuadStore
}

func NewAllIterator(prefix string, d quad.Direction, qs *QuadStore) *AllIterator {

	var rdr, _ = qs.dbreader()
	var iter = rdr.PrefixIterator0([]byte(prefix))
	return &AllIterator{
		uid:    iterator.NextUID(),
		iter:   iter,
		dir:    d,
		prefix: []byte(prefix),
		qs:     qs,
	}
}

func (it *AllIterator) UID() uint64 {
	return it.uid
}

func (it *AllIterator) Reset() {
	it.iter.Reset0()
}

func (it *AllIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *AllIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.iter.Value()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *AllIterator) Clone() graph.Iterator {
	out := NewAllIterator(string(it.prefix), it.dir, it.qs)
	out.tags.CopyFrom(it)
	return out
}

func (it *AllIterator) Next() bool {
	it.iter.Next()
	return it.iter.Valid()
}

func (it *AllIterator) Err() error {
	return it.iter.Error()
}

func (it *AllIterator) Result() graph.Value {
	return it.iter.Key()
}

func (it *AllIterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *AllIterator) Contains(v graph.Value) bool {
	return true
}

func (it *AllIterator) Close() error {
	return it.iter.Close()
}

func (it *AllIterator) Size() (int64, bool) {
	size, err := it.qs.SizeOfPrefix(it.prefix)
	if err == nil {
		return size, false
	}
	// INT64_MAX
	return int64(^uint64(0) >> 1), false
}

func (it *AllIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      size,
		Direction: it.dir,
	}
}

func (it *AllIterator) Type() graph.Type { return graph.All }
func (it *AllIterator) Sorted() bool     { return false }

func (it *AllIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *AllIterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
	}
}

var _ graph.Nexter = &AllIterator{}
