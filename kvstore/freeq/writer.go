/**
 * Use AppEngine DataStore for free, supposedly for enabled billing
 * NOTE: haven't tried this yet
 */

package freeq

import (
	"fmt"
	"log"

	"github.com/noypi/gae"
	"github.com/noypi/kv"
)

type Writer struct {
	ns     []byte //4 bytes nodeid (ex, id of namespace + username)
	logger gae.LogInt
	store  *Store
}

func (this *Writer) NewBatch() kv.KVBatch {
	b := new(Batch)
	b.ns = this.ns
	b.store = this.store
	b.merge = kv.NewEmulatedMerge(this.store.mo)
	b.logger = this.logger
	b.freeqMap = map[string]*freeqkv{}
	return b
}

func (this *Writer) NewBatchEx(options kv.KVBatchOptions) ([]byte, kv.KVBatch, error) {
	return make([]byte, options.TotalBytes), this.NewBatch(), nil
}

func (this *Writer) ExecuteBatch(batch kv.KVBatch) (err error) {

	dbi := this.store.dbi

	// first process merges
	if nil != batch.(*Batch).merge {
		for k, mergeOps := range batch.(*Batch).merge.Merges {
			kb := []byte(k)
			existingVal, err := getTrueValue(dbi, this.logger, this.ns, kb)
			if nil != err {
				return err
			}
			mergedVal, fullMergeOk := this.store.mo.FullMerge(kb, existingVal, mergeOps)
			if !fullMergeOk {
				return fmt.Errorf("merge operator returned failure")
			}

			batch.Set(kb, mergedVal)
		}
	}

	var toDelete, toPut gae.DbiKeyArr
	toPutsrc := []*struct{}{}
	putsrc := &struct{}{}
	for _, kv := range batch.(*Batch).freeqMap {
		if nil != kv.err {
			return kv.err
		}

		kraw := "g!" + string(kv.uuid)
		skbeg := kraw + "\x00\x00"
		skend := kraw + "\xff\xff"
		// delete existing value
		ks, _ := dbi.GetRangeKeys(skbeg, skend)
		if 0 < ks.Len() {
			toDelete = toDelete.AppendArr(ks)
		}

		// delete key
		if nil == kv.vrows {
			toDelete = toDelete.Append(dbi.CreateKey(string(kv.row), 0, nil))
			continue
		}

		// update existing value

		toPut = toPut.Append(dbi.CreateKey(string(kv.row), 0, nil))
		toPutsrc = append(toPutsrc, putsrc)
		if 0 == len(kv.vrows) {
			log.Fatal("vrows should not be empty, kv.row=", string(kv.row))
		}
		// insert value
		for _, bb := range kv.vrows {
			toPut = toPut.Append(dbi.CreateKey(string(bb), 0, nil))
			toPutsrc = append(toPutsrc, putsrc)
		}
	}
	if err = dbi.DeleteAll(toDelete); nil != err {
		this.logger.Criticalf("Error: ExecuteBatch DeleteAll=%v", err)
		return err
	}

	err = dbi.PutAll(toPut, toPutsrc)
	return err

}

func (this *Writer) Close() error {
	return nil
}
