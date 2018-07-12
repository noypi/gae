package kvbleve

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/noypi/gae"
	"github.com/noypi/gae/kvstore/freeq"
	"github.com/noypi/kv"
)

const Name = "noypi-kvbleve"

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	ns, has := config["namespace"].([]byte)
	if !has {
		if nss, has := config["namespace"].(string); has {
			ns = []byte(nss)
		}
	}
	dbint, _ := config["dbint"].(gae.DbExInt)
	logint, _ := config["logint"].(gae.LogInt)
	s, err := freeq.New(mo, logint, dbint, ns)
	return &_store{s}, err
}

//------------------
type _store struct {
	kvstore kv.KVStore
}

func (this *_store) Writer() (store.KVWriter, error) {
	w, err := this.kvstore.Writer()
	return &_writer{w}, err
}
func (this *_store) Reader() (store.KVReader, error) {
	r, err := this.kvstore.Reader()
	return &_reader{r}, err
}
func (this *_store) Close() error {
	return this.kvstore.Close()
}

//------------------
type _writer struct {
	kvwriter kv.KVWriter
}

func (this *_writer) NewBatch() store.KVBatch {
	return &_batch{this.kvwriter.NewBatch()}
}
func (this *_writer) NewBatchEx(opts store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	bb, batch, err := this.kvwriter.NewBatchEx(kv.KVBatchOptions{
		TotalBytes: opts.TotalBytes,
		NumSets:    opts.NumSets,
		NumDeletes: opts.NumDeletes,
		NumMerges:  opts.NumMerges,
	})
	return bb, &_batch{batch}, err
}
func (this *_writer) ExecuteBatch(batch store.KVBatch) error {
	return this.kvwriter.ExecuteBatch(batch.(*_batch).kvbatch)
}
func (this *_writer) Close() error {
	return this.kvwriter.Close()
}

//------------------
type _reader struct {
	kvreader kv.KVReader
}

func (this *_reader) Get(key []byte) ([]byte, error) {
	return this.kvreader.Get(key)
}
func (this *_reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return this.kvreader.MultiGet(keys)
}
func (this *_reader) PrefixIterator(prefix []byte) store.KVIterator {
	return &_iterator{this.kvreader.PrefixIterator(prefix)}
}
func (this *_reader) RangeIterator(start, end []byte) store.KVIterator {
	return &_iterator{this.kvreader.RangeIterator(start, end)}
}
func (this *_reader) Close() error {
	return this.kvreader.Close()
}

//------------------
type _batch struct {
	kvbatch kv.KVBatch
}

func (this *_batch) Set(key, val []byte) {
	this.kvbatch.Set(key, val)
}
func (this *_batch) Delete(key []byte) {
	this.kvbatch.Delete(key)
}
func (this *_batch) Merge(key, val []byte) {
	this.kvbatch.Merge(key, val)
}
func (this *_batch) Reset() {
	this.kvbatch.Reset()
}
func (this *_batch) Close() error {
	return this.kvbatch.Close()
}

//------------------
type _iterator struct {
	kviterator kv.KVIterator
}

func (this *_iterator) Seek(key []byte) {
	this.kviterator.Seek(key)
}
func (this *_iterator) Next() {
	this.kviterator.Next()
}
func (this *_iterator) Key() []byte {
	return this.kviterator.Key()
}
func (this *_iterator) Value() []byte {
	return this.kviterator.Value()
}
func (this *_iterator) Valid() bool {
	return this.kviterator.Valid()
}
func (this *_iterator) Current() ([]byte, []byte, bool) {
	return this.kviterator.Current()
}
func (this *_iterator) Close() error {
	return this.kviterator.Close()
}

func init() {
	registry.RegisterKVStore(Name, New)
}
