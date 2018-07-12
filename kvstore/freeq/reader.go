package freeq

import (
	"github.com/noypi/kv"
)

type Reader struct {
	store *Store
	ns    []byte
}

func (this *Reader) Get(key []byte) (v []byte, err error) {
	v, err = getTrueValue(this.store.dbi, this.store.logger, this.ns, key)
	return
}
func (this *Reader) MultiGet(keys [][]byte) (vv [][]byte, err error) {
	var v []byte
	for _, k := range keys {
		v, err = getTrueValue(this.store.dbi, this.store.logger, this.ns, k)
		vv = append(vv, v)
	}
	return
}
func (this *Reader) PrefixIterator(prefix []byte) (pt kv.KVIterator) {
	pt = this.prefixIterator(prefix, false)
	pt.Reset()
	return
}
func (this *Reader) PrefixIterator0(prefix []byte) (pt kv.KVIterator) {
	pt = this.prefixIterator(prefix, false)
	return
}
func (this *Reader) prefixIterator(prefix []byte, bReverse bool) (pt kv.KVIterator) {

	var send string
	if 0 < len(prefix) && 0xff > prefix[len(prefix)-1] {
		var endtmp = make([]byte, len(prefix))
		copy(endtmp, prefix)
		endtmp[len(endtmp)-1] = endtmp[len(endtmp)-1] + 1
		send = string(endtmp)
	} else {
		send = "\xff"
	}

	sbeg := string(genNsKeyPrefix(this.ns, prefix, []byte{}))
	beginK := this.store.dbi.CreateKey(sbeg, 0, nil)
	send = string(genNsKeyPrefix(this.ns, []byte(send), []byte{}))
	endK := this.store.dbi.CreateKey(send, 0, nil)

	q := this.store.dbi.NewQuery().KeysOnly()
	if bReverse {
		q = q.Order("-__key__")
	}
	q = q.Filter("__key__ >=", beginK).Filter("__key__ <", endK)

	this.store.logger.Debugf("sbeg=%s, endK=%s\n", sbeg, send)

	pt = &Iterator{
		prefix:  prefix,
		ns:      this.ns,
		store:   this.store,
		reverse: bReverse,
		q:       q,
	}

	//nCount, _ := q.Count(this.store.dbi.Context())
	//this.store.logger.Debugf("prefixIterator pre=%s, count=%d", string(prefix), nCount)

	return pt
}
func (this *Reader) RangeIterator(start, end []byte) (rt kv.KVIterator) {
	rt = this.rangeIterator(start, end, false)
	rt.Reset()
	return
}
func (this *Reader) RangeIterator0(start, end []byte) (rt kv.KVIterator) {
	rt = this.rangeIterator(start, end, false)
	return
}
func (this *Reader) rangeIterator(start, end []byte, bReverse bool) kv.KVIterator {

	q := this.store.dbi.NewQuery().KeysOnly()
	if bReverse {
		q = q.Order("-__key__")
	}

	var endtmp []byte
	if 0 < len(end) && 0xff > end[len(end)-1] {
		endtmp = make([]byte, len(end))
		copy(endtmp, end)
		endtmp[len(endtmp)-1] = endtmp[len(endtmp)-1] + 1
	}

	beginx := genNsKeyPrefix(this.ns, start, []byte{})
	beginK := this.store.dbi.CreateKey(string(beginx), 0, nil)

	endx := genNsKeyPrefix(this.ns, endtmp, []byte{})
	endK := this.store.dbi.CreateKey(string(endx), 0, nil)

	q = q.Filter("__key__ >=", beginK).Filter("__key__<", endK)
	pt := &Iterator{
		begin:   start,
		end:     end,
		ns:      this.ns,
		store:   this.store,
		reverse: bReverse,
		q:       q,
	}
	return pt

}

func (this *Reader) ReversePrefixIterator(prefix []byte) (t kv.KVIterator) {
	t = this.prefixIterator(prefix, true)
	t.Reset()
	return
}

func (this *Reader) ReverseRangeIterator(start, end []byte) (t kv.KVIterator) {
	t = this.rangeIterator(start, end, true)
	t.Reset()
	return
}

func (this *Reader) Close() error {
	return nil
}
