package freeq

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"errors"

	"github.com/golang/snappy"
	"github.com/noypi/gae"
	"github.com/twinj/uuid"
)

const C_Klimit = 500
const C_Kreserved = 16
const C_Kmincompress = 120
const C_Kmetasize = 4
const C_Ksepbyte = 0x7f

var C_Kterminator = []byte{0x7f, 0x7f}

var C_Vlimit = (1024 << 10) // 1MB

var ErrInvalidValue = errors.New("invalid size for value.")

type freeqkv struct {
	vrows [][]byte
	ns    []byte // namespace
	uuid  []byte
	row   []byte // row needed for query (using iterator)
	err   error
}

func (this *freeqkv) genKeyRequirement(ns, k []byte) {
	this.ns = ns
	this.uuid = genuuid(ns, k)
	// create k
	this.row = genNsKeyPrefix(ns, k, this.uuid)
}

func (this *Batch) newFreeq(k, v []byte) (kv *freeqkv) {
	kv = new(freeqkv)
	kv.genKeyRequirement(this.ns, k)

	v = snappy.Encode(nil, v)
	v = kvencode(v)

	//---
	var n uint16 = 1
	bbBuf := bytes.NewBufferString("g!")
	bbBuf.Write(kv.uuid)
	nPrefixLen := bbBuf.Len()

	// create v
	// 2 bytes of counter, compress flag is last bit => 0x1000

	//-- if can fit in one row
	var limit = C_Klimit - (nPrefixLen + C_Kreserved + C_Kmetasize + 1) // separator + (flag and counter)
	if len(v) <= limit {
		bbBuf.Write(C_Kterminator)
		bbBuf.Write(v)
		kv.vrows = [][]byte{bbBuf.Bytes()}
		return
	}

	//this.logger.Debugf("v=%s", string(v))

	//-- if more than limit per row
	for {
		if (0x8000 & n) == 0x8000 {
			kv.err = ErrInvalidValue
			break
		}

		var nn = make([]byte, 2)
		binary.BigEndian.PutUint16(nn, n)
		bbBuf.Truncate(nPrefixLen)
		if limit <= len(v) {
			bbBuf.Write(nn)
			bbBuf.Write(v[0:limit])
			var bb = make([]byte, bbBuf.Len())
			copy(bb, bbBuf.Bytes())
			kv.vrows = append(kv.vrows, bb)
		} else {
			bbBuf.Write(C_Kterminator)
			bbBuf.Write(v)
			var bb = make([]byte, bbBuf.Len())
			copy(bb, bbBuf.Bytes())
			kv.vrows = append(kv.vrows, bb)
			break
		}

		v = v[limit:]
		n++
		if (n & 0x0080) == 0x0080 {
			n |= 0x00ff
			n++
		}
	}

	return kv
}

func genuuid(ns, k []byte) (id []byte) {
	k = kvencode(k)
	uuidname := uuid.Name(fmt.Sprintf("/%s/%s", string(ns), string(k)))
	bbGuid := uuid.NewV5(uuid.NameSpaceURL, uuidname).Bytes()
	id = kvencode(bbGuid)
	return
}

// should be a util function
func genNsKeyPrefix(ns, k, extra []byte) []byte {
	bufk := bytes.NewBufferString("ns")
	bufk.WriteByte(C_Ksepbyte)
	bufk.Write(ns)
	bufk.WriteByte(C_Ksepbyte)
	k = kvencode(k)
	bufk.Write(k)
	bufk.Write([]byte{0x00})
	if 0 < len(extra) {
		bufk.WriteByte(C_Ksepbyte)
		bufk.Write(extra)
	}
	return bufk.Bytes()
}

// should be a util function
func getTrueValue(dbi gae.DbExInt, logger gae.LogInt, ns, k []byte) (trueval []byte, err error) {
	uuid := genuuid(ns, k)
	skraw := "g!" + string(uuid)
	ks, err := dbi.GetRangeKeys(skraw+"\x00\x00", skraw+"\xff\xff")
	if nil != err {
		return
	}

	buf := bytes.NewBuffer([]byte{})
	ks.ForEach(func(kraw gae.DbiKey) bool {
		name := kraw.StringID()
		buf.Write([]byte(name[len(skraw)+2:])) // 24(g!guid) + 2(counter)
		return true
	})
	trueval = buf.Bytes()
	//logger.Debugf("getTrueValue trueval=%s", string(trueval))
	if trueval, err = kvdecode(trueval); nil != err {
		return
	}
	trueval, err = snappy.Decode(nil, trueval)
	return
}
