package kvcayley

import (
	"sort"

	"github.com/noypi/gae"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/writer"

	"context"

	"github.com/noypi/gae/dbi/std"
	"github.com/noypi/gae/kvstore"
	"github.com/noypi/gae/kvstore/freeq"
	"github.com/noypi/gae/logi/std"
	"github.com/noypi/gae/registry"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"

	. "gopkg.in/check.v1"
)

func makeQuadSet() []quad.Quad {
	quadSet := []quad.Quad{
		{"A", "follows", "B", ""},
		{"C", "follows", "B", ""},
		{"C", "follows", "D", ""},
		{"D", "follows", "B", ""},
		{"B", "follows", "F", ""},
		{"F", "follows", "G", ""},
		{"D", "follows", "G", ""},
		{"E", "follows", "F", ""},
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	return quadSet
}

func iteratedQuads(qs graph.QuadStore, it graph.Iterator) []quad.Quad {
	var res ordered
	for graph.Next(it) {
		quadof := qs.Quad(it.Result())
		qs.(*QuadStore).logger.Debugf("iteratedQuads quadof=%v", quadof)
		res = append(res, quadof)
	}
	sort.Sort(res)
	qs.(*QuadStore).logger.Debugf("iteratedQuads res=%v", res)
	return res
}

func iteratedNames(qs graph.QuadStore, it graph.Iterator) []string {
	var res []string
	for graph.Next(it) {
		nameof := qs.NameOf(it.Result())
		qs.(*QuadStore).logger.Debugf("iteratedNames nameof=%s", nameof)
		res = append(res, nameof)
	}
	qs.(*QuadStore).logger.Debugf("iteratedNames res=%v", res)
	sort.Strings(res)
	return res
}

type ordered []quad.Quad

func (o ordered) Len() int { return len(o) }
func (o ordered) Less(i, j int) bool {
	switch {
	case o[i].Subject < o[j].Subject,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate < o[j].Predicate,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate == o[j].Predicate &&
			o[i].Object < o[j].Object,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate == o[j].Predicate &&
			o[i].Object == o[j].Object &&
			o[i].Label < o[j].Label:

		return true

	default:
		return false
	}
}
func (o ordered) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func getStore(c *C) (store kvstore.KVStore, db gae.DbInt, logger gae.LogInt, fncloser func()) {
	var err error
	var ctx context.Context
	ctx, fncloser, err = aetest.NewContext(&aetest.Options{"", true})
	c.Assert(err, IsNil)

	logger, err = registry.GetLogi(logi.Name, map[string]interface{}{
		"context": ctx,
	})
	c.Assert(err, IsNil)

	db, err = registry.GetDbi(dbi.Name, map[string]interface{}{
		"logger":  logger,
		"context": ctx,
		"appid":   appengine.AppID(ctx),
		"name":    "mytestdb",
	})
	c.Assert(err, IsNil)

	store, err = freeq.New(nil, logger, db, []byte("mynamespace"))
	c.Assert(err, IsNil)
	return
}

func getQuadStore(c *C) (qs graph.QuadStore, logger gae.LogInt, fncloser func(), opts graph.Options) {
	var namespace = "testcayley"
	_, db, logger, fncloser := getStore(c)

	opts = graph.Options{
		"context":   db.Context(),
		"logger":    logger,
		"namespace": namespace,
	}

	qs, err := newQuadStore("mykind", opts)
	c.Assert(err, IsNil)
	c.Assert(qs, NotNil)
	return
}

func (suite *MySuite) TestCreateDatabase(c *C) {

	var qs, _, fncloser, _ = getQuadStore(c)
	defer fncloser()
	defer qs.Close()
	if s := qs.Size(); s != 0 {
		c.Fatalf("Unexpected size, got:%d expected:0", s)
	}

}

func (suite *MySuite) TestLoadDatabase(c *C) {

	var qs, _, fncloser, opts = getQuadStore(c)
	defer fncloser()

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuad(quad.Quad{
		Subject:   "Something",
		Predicate: "points_to",
		Object:    "Something Else",
		Label:     "context",
	})
	for _, pq := range []string{"Something", "points_to", "Something Else", "context"} {
		got := qs.NameOf(qs.ValueOf(pq))
		c.Assert(got, Equals, pq)
	}
	s := qs.Size()
	c.Assert(s, Equals, int64(1))

	qs.Close()

	qs, err := newQuadStore("mykind", opts)
	c.Assert(err, IsNil)
	c.Assert(qs, NotNil)

	w, _ = writer.NewSingleReplication(qs, nil)

	ts2, didConvert := qs.(*QuadStore)
	if !didConvert {
		c.Fatalf("Could not convert from generic to LevelDB QuadStore")
	}

	//Test horizon
	horizon := qs.Horizon()
	if horizon.Int() != 1 {
		c.Fatalf("Unexpected horizon value, got:%d expect:1", horizon.Int())
	}

	w.AddQuadSet(makeQuadSet())
	if s := qs.Size(); s != 12 {
		c.Fatalf("Unexpected quadstore size, got:%d expect:12", s)
	}
	if s := ts2.SizeOf(qs.ValueOf("B")); s != 5 {
		c.Fatalf("Unexpected quadstore size, got:%d expect:5", s)
	}
	horizon = qs.Horizon()
	if horizon.Int() != 12 {
		c.Fatalf("Unexpected horizon value, got:%d expect:12", horizon.Int())
	}

	w.RemoveQuad(quad.Quad{
		Subject:   "A",
		Predicate: "follows",
		Object:    "B",
		Label:     "",
	})
	if s := qs.Size(); s != 11 {
		c.Fatalf("Unexpected quadstore size after RemoveQuad, got:%d expect:11", s)
	}
	if s := ts2.SizeOf(qs.ValueOf("B")); s != 4 {
		c.Fatalf("Unexpected quadstore size, got:%d expect:4", s)
	}

}

func (suite *MySuite) TestIterator(c *C) {
	qs, _, fncloser, _ := getQuadStore(c)
	defer fncloser()
	defer qs.Close()

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())
	var it graph.Iterator

	it = qs.NodesAllIterator()
	c.Assert(it, NotNil)

	size, exact := it.Size()
	if size <= 0 || size >= 20 {
		c.Fatalf("Unexpected size, got:%d expect:(0, 20)", size)
	}
	if exact {
		c.Fatalf("Got unexpected exact result.")
	}
	if typ := it.Type(); typ != graph.All {
		c.Fatalf("Unexpected iterator type, got:%v expect:%v", typ, graph.All)
	}
	optIt, changed := it.Optimize()
	if changed || optIt != it {
		c.Fatalf("Optimize unexpectedly changed iterator.")
	}

	expect := []string{
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"follows",
		"status",
		"cool",
		"status_graph",
	}
	sort.Strings(expect)
	for i := 0; i < 2; i++ {
		got := iteratedNames(qs, it)
		sort.Strings(got)
		c.Assert(got, DeepEquals, expect)
		it.Reset()
	}

	for _, pq := range expect {
		if !it.Contains(qs.ValueOf(pq)) {
			c.Fatalf("Failed to find and check %q correctly", pq)
		}
	}
	it.Reset()

	it = qs.QuadsAllIterator()
	graph.Next(it)
	q := qs.Quad(it.Result())
	set := makeQuadSet()
	var ok bool
	for _, t := range set {
		if t.String() == q.String() {
			ok = true
			break
		}
	}
	if !ok {
		c.Fatalf("Failed to find %q during iteration, got:%q", q, set)
	}

}

func (suite *MySuite) TestSetIterator(c *C) {

	qs, _, fncloser, _ := getQuadStore(c)
	defer fncloser()
	defer qs.Close()

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())

	expect := []quad.Quad{
		{"C", "follows", "B", ""},
		{"C", "follows", "D", ""},
	}
	sort.Sort(ordered(expect))

	// Subject iterator.
	it := qs.QuadIterator(quad.Subject, qs.ValueOf("C"))

	got := iteratedQuads(qs, it)
	c.Assert(got, DeepEquals, expect)
	it.Reset()

	and := iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadsAllIterator())
	and.AddSubIterator(it)

	got = iteratedQuads(qs, and)
	c.Assert(got, DeepEquals, expect)

	// Object iterator.
	it = qs.QuadIterator(quad.Object, qs.ValueOf("F"))

	expect = []quad.Quad{
		{"B", "follows", "F", ""},
		{"E", "follows", "F", ""},
	}
	sort.Sort(ordered(expect))
	got = iteratedQuads(qs, it)
	c.Assert(got, DeepEquals, expect)

	and = iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf("B")))
	and.AddSubIterator(it)

	expect = []quad.Quad{
		{"B", "follows", "F", ""},
	}
	got = iteratedQuads(qs, and)
	c.Assert(got, DeepEquals, expect)

	// Predicate iterator.
	it = qs.QuadIterator(quad.Predicate, qs.ValueOf("status"))

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	sort.Sort(ordered(expect))
	got = iteratedQuads(qs, it)
	c.Assert(got, DeepEquals, expect)

	// Label iterator.
	it = qs.QuadIterator(quad.Label, qs.ValueOf("status_graph"))

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	sort.Sort(ordered(expect))
	got = iteratedQuads(qs, it)
	c.Assert(got, DeepEquals, expect)
	it.Reset()

	// Order is important
	and = iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf("B")))
	and.AddSubIterator(it)

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
	}
	got = iteratedQuads(qs, and)
	c.Assert(got, DeepEquals, expect)
	it.Reset()

	// Order is important
	and = iterator.NewAnd(qs)
	and.AddSubIterator(it)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf("B")))

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
	}
	got = iteratedQuads(qs, and)
	c.Assert(got, DeepEquals, expect)
}

func (suite *MySuite) TestOptimize(c *C) {
	qs, _, fncloser, _ := getQuadStore(c)
	defer fncloser()
	defer qs.Close()

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())

	// With an linksto-fixed pair
	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf("F"))
	fixed.Tagger().Add("internal")
	lto := iterator.NewLinksTo(qs, fixed, quad.Object)

	oldIt := lto.Clone()
	newIt, ok := lto.Optimize()
	if !ok {
		c.Fatalf("Failed to optimize iterator")
	}
	if newIt.Type() != Type() {
		c.Fatalf("Optimized iterator type does not match original, got:%v expect:%v", newIt.Type(), Type())
	}

	newQuads := iteratedQuads(qs, newIt)
	oldQuads := iteratedQuads(qs, oldIt)
	c.Assert(newQuads, DeepEquals, oldQuads)

	graph.Next(oldIt)
	oldResults := make(map[string]graph.Value)
	oldIt.TagResults(oldResults)
	graph.Next(newIt)
	newResults := make(map[string]graph.Value)
	newIt.TagResults(newResults)
	c.Assert(newResults, DeepEquals, oldResults)
}
