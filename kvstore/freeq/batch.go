package freeq

import (
	"github.com/noypi/gae"
	"github.com/noypi/kv"
)

type Batch struct {
	freeqMap map[string]*freeqkv
	ns       []byte
	merge    *kv.EmulatedMerge
	store    *Store
	logger   gae.LogInt
}

func (this *Batch) Set(key, val []byte) {
	kv := this.newFreeq(key, val)
	this.freeqMap[string(kv.uuid)] = kv
}

func (this *Batch) Delete(key []byte) {
	kv := new(freeqkv)
	kv.genKeyRequirement(this.ns, key)
	kv.vrows = nil
	this.freeqMap[string(kv.uuid)] = kv
}

func (this *Batch) Merge(key, val []byte) {
	this.merge.Merge(key, val)
}

func (this *Batch) Reset() {
	this.freeqMap = map[string]*freeqkv{}
	this.merge = kv.NewEmulatedMerge(this.store.mo)
}

func (this *Batch) Close() error {
	this.freeqMap = nil
	this.merge = nil
	return nil
}
