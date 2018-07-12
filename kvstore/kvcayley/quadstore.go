package kvcayley

//go:generate ffjson quadstore.go

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"sync"

	json "github.com/pquerna/ffjson/ffjson"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"

	"github.com/noypi/gae"
	"github.com/noypi/gae/dbi/std"
	"github.com/noypi/gae/kvstore/freeq"
	"github.com/noypi/gae/logi/std"
	"github.com/noypi/gae/registry"
	"github.com/noypi/kv"

	"context"

	"google.golang.org/appengine"
)

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:           newQuadStore,
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		InitFunc:          createNewKVFreeqDB,
		IsPersistent:      true,
	})
}

const (
	DefaultCacheSize       = 2
	DefaultWriteBufferSize = 20
	QuadStoreType          = "kvcayley-freeq"
	horizonKey             = "__horizon"
	sizeKey                = "__size"
)

var (
	hashPool = sync.Pool{
		New: func() interface{} { return sha1.New() },
	}
	hashSize = sha1.Size
)

type Token struct {
	Key      string
	Property string
}

type QuadStore struct {
	dbint gae.DbInt
	db    kv.KVStore

	logger gae.LogInt

	open    bool
	size    int64
	horizon int64
}

func createNewKVFreeqDB(path string, _ graph.Options) error {
	return nil
}

func newQuadStore(kind string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	var err error

	ctx := options["context"].(context.Context)
	if nil == ctx {
		return nil, gae.ErrInvalidParams("context")
	}
	logger := options["logger"].(gae.LogInt)
	if nil == ctx {
		return nil, gae.ErrInvalidParams("logger")
	}
	qs.logger = logger

	logger, err = registry.GetLogi(logi.Name, map[string]interface{}{
		"context": ctx,
	})
	if nil != err {
		return nil, err
	}

	dbint, err := registry.GetDbiEx(dbi.Name, map[string]interface{}{
		"logger":  logger,
		"context": ctx,
		"appid":   appengine.AppID(ctx),
		"name":    kind,
	})
	if nil != err {
		return nil, err
	}
	if nil != err {
		return nil, err
	}

	var namespace = options["namespace"].(string)
	if 0 == len(namespace) {
		namespace = "/default/"
	}

	qs.dbint = dbint
	qs.db, err = freeq.New(nil, logger, dbint, []byte(namespace))
	if nil != err {
		return nil, err
	}
	err = qs.getMetadata()

	return &qs, nil
}

func (qs *QuadStore) Size() int64 {
	return qs.size
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	return graph.NewSequentialKey(qs.horizon)
}

func hashOf(s string) []byte {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	key := make([]byte, 0, hashSize)
	h.Write([]byte(s))
	key = h.Sum(key)
	return key
}

func (qs *QuadStore) createKeyFor(d [4]quad.Direction, q quad.Quad) []byte {
	key := make([]byte, 0, 2+(hashSize*4))
	// TODO(kortschak) Remove dependence on String() method.
	key = append(key, []byte{d[0].Prefix(), d[1].Prefix()}...)
	key = append(key, hashOf(q.Get(d[0]))...)
	key = append(key, hashOf(q.Get(d[1]))...)
	key = append(key, hashOf(q.Get(d[2]))...)
	key = append(key, hashOf(q.Get(d[3]))...)
	return key
}

func (qs *QuadStore) createValueKeyFor(s string) []byte {
	key := make([]byte, 0, 1+hashSize)
	key = append(key, []byte("z")...)
	key = append(key, hashOf(s)...)
	return key
}

type IndexEntry struct {
	quad.Quad
	History []int64
}

// Short hand for direction permutations.
var (
	spo = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
	osp = [4]quad.Direction{quad.Object, quad.Subject, quad.Predicate, quad.Label}
	pos = [4]quad.Direction{quad.Predicate, quad.Object, quad.Subject, quad.Label}
	cps = [4]quad.Direction{quad.Label, quad.Predicate, quad.Subject, quad.Object}
)

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) (err error) {
	wrtr, err := qs.db.Writer()
	if nil != err {
		return
	}
	var batch = wrtr.NewBatch()
	resizeMap := make(map[string]int64)
	sizeChange := int64(0)
	for _, d := range deltas {
		if d.Action != graph.Add && d.Action != graph.Delete {
			return fmt.Errorf("Error: ApplyDeltas invalid action")
		}
		bytes, err := json.Marshal(d)
		if err != nil {
			return err
		}
		batch.Set(keyFor(d), bytes)
		err = qs.buildQuadWrite(batch, d.Quad, d.ID.Int(), d.Action == graph.Add)
		if err != nil {
			if err == graph.ErrQuadExists && ignoreOpts.IgnoreDup {
				continue
			}
			if err == graph.ErrQuadNotExist && ignoreOpts.IgnoreMissing {
				continue
			}
			return err
		}
		delta := int64(1)
		if d.Action == graph.Delete {
			delta = int64(-1)
		}
		resizeMap[d.Quad.Subject] += delta
		resizeMap[d.Quad.Predicate] += delta
		resizeMap[d.Quad.Object] += delta
		if d.Quad.Label != "" {
			resizeMap[d.Quad.Label] += delta
		}
		sizeChange += delta
		qs.horizon = d.ID.Int()
	}
	for k, v := range resizeMap {
		if v != 0 {
			err := qs.UpdateValueKeyBy(k, v, batch)
			if err != nil {
				return err
			}
		}
	}
	err = wrtr.ExecuteBatch(batch)
	if nil != err {
		qs.logger.Errorf("Error: could not write to DB for quadset.")
		return err
	}
	qs.size += sizeChange
	return nil
}

func keyFor(d graph.Delta) []byte {
	key := make([]byte, 0, 19)
	key = append(key, 'd')
	key = append(key, []byte(fmt.Sprintf("%018x", d.ID.Int()))...)
	return key
}

func (qs *QuadStore) buildQuadWrite(batch kv.KVBatch, q quad.Quad, id int64, isAdd bool) (err error) {
	var entry IndexEntry
	rdr, err := qs.db.Reader()
	if nil != err {
		return
	}
	data, err := rdr.Get(qs.createKeyFor(spo, q))
	if nil != err {
		qs.logger.Errorf("Error: could not access DB to prepare index: %v", err)
		return err
	}
	if nil == err && 0 < len(data) {
		// We got something.
		err = entry.UnmarshalJSON(data)
		if err != nil {
			return err
		}
	} else {
		entry.Quad = q
	}

	if isAdd && len(entry.History)%2 == 1 {
		qs.logger.Errorf("Error: attempt to add existing quad %v: %#v", entry, q)
		return graph.ErrQuadExists
	}
	if !isAdd && len(entry.History)%2 == 0 {
		qs.logger.Errorf("Error: attempt to delete non-existent quad %v: %#c", entry, q)
		return graph.ErrQuadNotExist
	}

	entry.History = append(entry.History, id)

	bytes, err := entry.MarshalJSON()
	if nil != err {
		qs.logger.Errorf("Error: could not write to buffer for entry %#v: %s", entry, err)
		return err
	}
	batch.Set(qs.createKeyFor(spo, q), bytes)
	batch.Set(qs.createKeyFor(osp, q), bytes)
	batch.Set(qs.createKeyFor(pos, q), bytes)
	if q.Get(quad.Label) != "" {
		batch.Set(qs.createKeyFor(cps, q), bytes)
	}
	return nil
}

type ValueData struct {
	Name string
	Size int64
}

func (qs *QuadStore) UpdateValueKeyBy(name string, amount int64, batch kv.KVBatch) (err error) {
	value := &ValueData{name, amount}
	key := qs.createValueKeyFor(name)

	rdr, err := qs.db.Reader()
	if nil != err {
		return
	}
	b, err := rdr.Get(key)

	// Error getting the node from the database.
	if nil != err {
		qs.logger.Errorf("Error: reading Value %s from the DB.", name)
		return err
	}

	// Node exists in the database -- unmarshal and update.
	if 0 < len(b) {
		err = value.UnmarshalJSON(b)
		if err != nil {
			qs.logger.Errorf("Error: could not reconstruct value: %v", err)
			return err
		}
		value.Size += amount
	}

	// Are we deleting something?
	if value.Size <= 0 {
		value.Size = 0
	}

	// Repackage and rewrite.
	bytes, err := value.MarshalJSON()
	if err != nil {
		qs.logger.Errorf("could not write to buffer for value %s: %s", name, err)
		return err
	}
	if batch == nil {
		var wrtr kv.KVWriter
		wrtr, err = qs.db.Writer()
		if nil != err {
			return
		}
		var batch2 = wrtr.NewBatch()
		batch2.Set(key, bytes)
		err = wrtr.ExecuteBatch(batch2)
	} else {
		batch.Set(key, bytes)
	}
	return nil
}

func (qs *QuadStore) Close() {
	buf := new(bytes.Buffer)
	var wrtr, err = qs.db.Writer()
	if nil != err {
		qs.logger.Errorf("Error: Close(), db.Writer() %v", err)
		return
	}
	err = binary.Write(buf, binary.LittleEndian, qs.size)
	if err == nil {
		batch := wrtr.NewBatch()
		batch.Set([]byte(sizeKey), buf.Bytes())
		werr := wrtr.ExecuteBatch(batch)
		if werr != nil {
			qs.logger.Errorf("Error: could not write size before closing!")
		}
	} else {
		qs.logger.Errorf("Error: could not convert size before closing!")
	}
	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, qs.horizon)
	if err == nil {
		batch := wrtr.NewBatch()
		batch.Set([]byte(horizonKey), buf.Bytes())
		werr := wrtr.ExecuteBatch(batch)
		if werr != nil {
			qs.logger.Errorf("Error: could not write horizon before closing!")
		}
	} else {
		qs.logger.Errorf("Error: could not convert horizon before closing!")
	}
	qs.db.Close()
	qs.open = false
}

func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	var q quad.Quad

	rdr, _ := qs.dbreader()
	b, err := rdr.Get(k.([]byte))
	if err != nil {
		qs.logger.Errorf("Error: could not get quad from DB.")
		return quad.Quad{}
	}
	if 0 == len(b) {
		// No harm, no foul.
		return quad.Quad{}
	}
	err = json.Unmarshal(b, &q)
	if err != nil {
		qs.logger.Errorf("Error: could not reconstruct quad. %v", err)
		return quad.Quad{}
	}
	return q
}

func (qs *QuadStore) ValueOf(s string) graph.Value {
	return qs.createValueKeyFor(s)
}

func (qs *QuadStore) valueData(k []byte) ValueData {
	var out ValueData
	rdr, _ := qs.dbreader()
	b, err := rdr.Get(k)
	if err != nil {
		qs.logger.Errorf("Error: could not get value from DB")
		return out
	}
	if 0 < len(b) {
		err := out.UnmarshalJSON(b)
		if err != nil {
			qs.logger.Errorf("Error: could not reconstruct value=%s, %v", string(b), err)
			return ValueData{}
		}
	}
	return out
}

func (qs *QuadStore) NameOf(v graph.Value) (s string) {
	if 0 == len(v.([]byte)) {
		return ""
	}
	return qs.valueData(v.([]byte)).Name
}

func (qs *QuadStore) SizeOf(v graph.Value) (n int64) {
	if 0 == len(v.([]byte)) {
		return 0
	}
	return int64(qs.valueData(v.([]byte)).Size)
}

func (qs *QuadStore) getInt64ForKey(key string, empty int64) (int64, error) {
	var out int64
	var rdr, err = qs.dbreader()
	if nil != err {
		return 0, err
	}
	b, err := rdr.Get([]byte(key))
	if err != nil {
		qs.logger.Errorf("Error: could not read %s: %s", key, err.Error())
		return 0, err
	}
	if 0 == len(b) {
		// Must be a new database. Cool
		return empty, nil
	}
	buf := bytes.NewBuffer(b)
	err = binary.Read(buf, binary.LittleEndian, &out)
	if err != nil {
		qs.logger.Errorf("Error: could not parse %s: %s", key, err.Error())
		return 0, err
	}
	return out, nil
}

func (qs *QuadStore) getMetadata() error {
	var err error
	qs.size, err = qs.getInt64ForKey(sizeKey, 0)
	if err != nil {
		return err
	}
	qs.horizon, err = qs.getInt64ForKey(horizonKey, 0)
	return err
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	var prefix string
	switch d {
	case quad.Subject:
		prefix = "sp"
	case quad.Predicate:
		prefix = "po"
	case quad.Object:
		prefix = "os"
	case quad.Label:
		prefix = "cp"
	default:
		panic("unreachable " + d.String())
	}
	return NewIterator(prefix, d, val, qs)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator("z", quad.Any, qs)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator("po", quad.Predicate, qs)
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	v := val.([]byte)
	offset := PositionOf(v[0:2], d, qs)
	if offset != -1 {
		return append([]byte("z"), v[offset:offset+hashSize]...)
	}
	return qs.Quad(val).Get(d)
}

func (qs *QuadStore) SizeOfPrefix(pre []byte) (int64, error) {
	rdr, err := qs.dbreader()
	if nil != err {
		return 0, err
	}
	iter := rdr.PrefixIterator0(pre)
	return int64(iter.Count()), nil
}

func compareBytes(a, b graph.Value) bool {
	return bytes.Equal(a.([]byte), b.([]byte))
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(compareBytes)
}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}

func (qs *QuadStore) dbreader() (rdr kv.KVReader, err error) {
	if rdr, err = qs.db.Reader(); nil != err {
		qs.logger.Errorf("Error: db.Reader() %v", err)
	}
	return
}
