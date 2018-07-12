package freeq

import (
	"bytes"
	"fmt"

	"github.com/noypi/gae"
)

type Iterator struct {
	it                 gae.DbiIterator
	store              *Store
	prefix, begin, end []byte
	ns                 []byte
	currerr            error
	currkey            gae.DbiKey
	k, v               []byte
	reverse            bool
	q                  gae.DbiQuery
}

func (this Iterator) Error() error {
	return this.currerr
}

func (this *Iterator) Reset() {
	this.store.logger.Debugf("+Iterator.Reset()")
	defer this.store.logger.Debugf("-Iterator.Reset()")
	this.Reset0()
	if this.reverse {
		if 0 < len(this.prefix) {
			this.Seek(this.prefix)
		} else {
			this.Seek(this.begin)
		}
	} else {
		this.Next()
	}
}

func (this *Iterator) Reset0() {
	this.store.logger.Debugf("+Iterator.Reset0()")
	defer this.store.logger.Debugf("-Iterator.Reset0()")
	this.k = nil
	this.v = nil
	this.currerr = nil
	this.it = this.store.dbi.Run(this.q)
}

func (this *Iterator) Seek(key []byte) {

	for {
		if 0 < bytes.Compare(this.k, key) || nil != this.currerr {
			// k greater key
			// or error
			break
		}
		this.next()
	}
	return
}

func (this *Iterator) next() {
	//this.store.logger.Debugf("Next prefix=%s, begin=%s, end=%s", string(this.prefix), string(this.begin), string(this.end))
	this.k = nil
	this.v = nil
	if nil != this.currerr {
		return
	}
	this.currkey, this.currerr = this.it.Next(nil)
	if this.currkey.IsNil() {
		if nil == this.currerr {
			this.currerr = fmt.Errorf("nothing found")
		}
		return
	}

	if len(this.ns) < len(this.currkey.StringID()) {
		var name = this.currkey.StringID()
		var kv = bytes.SplitN([]byte(name)[len(this.ns)+4:], []byte{0x00}, 2)
		if 1 < len(kv) {
			this.k, _ = kvdecode(kv[0])
		} else {
			this.currerr = fmt.Errorf("internal error, invalid key format name=%s", name)
			this.k = nil
		}
	} else {
		this.k = nil
	}

	//this.store.logger.Debugf("freeq.iterator Next() v=%s", string(this.Value()))

}

func (this *Iterator) Next() {
	if this.it.IsNil() {
		this.it = this.store.dbi.Run(this.q)
	}

	this.next()

	if 0 < len(this.k) && nil == this.currerr {
		var bDone bool = (0 < len(this.prefix) && !bytes.HasPrefix(this.k, this.prefix))
		if !bDone && (0 < len(this.begin)) {
			if this.reverse {
				bDone = string(this.k) < string(this.begin)
				//this.store.logger.Debugf("k=%s, begin=%s. k<begin=%v.... end=%s", string(this.k), string(this.begin), bDone, string(this.end))
			} else {
				bDone = string(this.k) > string(this.end) && !bytes.HasPrefix(this.k, this.end)
				//this.store.logger.Debugf("k=%s, begin=%s. k>end=%v.... end=%s", string(this.k), string(this.begin), bDone, string(this.end))
			}

		}
		if bDone {
			//this.store.logger.Debugf("done k=%s, prefix=%s, begin=%s, end=%s", string(this.k), string(this.prefix), string(this.begin), string(this.end))
			this.currerr = fmt.Errorf("done")
			this.k = nil
		}
	}

}

func (this *Iterator) Count() (n int) {
	n, _ = this.q.Count(this.store.dbi.Context())
	return
}

func (this *Iterator) Current() ([]byte, []byte, bool) {
	k := this.Key()
	v := this.Value()
	//this.store.logger.Debugf("Current k=%s, v=%s", string(k), string(v))
	return k, v, this.Valid()
}

func (this *Iterator) Key() []byte {
	return this.k
}

func (this *Iterator) Value() []byte {
	if nil == this.v && 0 < len(this.k) {
		this.v, this.currerr = getTrueValue(this.store.dbi, this.store.logger, this.ns, this.k)
	}
	return this.v
}

func (this *Iterator) Valid() bool {
	//if nil != this.currerr {
	//	this.store.logger.Debugf("currerr=%s", this.currerr)
	//}
	return nil == this.currerr
}

func (this *Iterator) Close() error {
	return nil
}
