package dbi

import (
	"context"

	"github.com/noypi/gae"
	"github.com/noypi/gae/registry"
)

const Name = "dummy"

type dummydbi struct {
	c context.Context
}

// required params:
//     context context.Context
func New(params map[string]interface{}) (db gae.DbInt, err error) {
	if nil == params {
		db = newDummyDbi(nil)
	} else {
		c, _ := params["context"]
		db = newDummyDbi(c.(context.Context))
	}
	return
}

func NewEx(params map[string]interface{}) (db gae.DbExInt, err error) {
	c, has := params["context"]
	if !has {
		return nil, gae.ErrInvalidParams("context")
	}
	db = newDummyDbi(c.(context.Context))
	return
}

func newDummyDbi(c context.Context) *dummydbi {
	o := new(dummydbi)
	o.c = c
	return o
}

func (this dummydbi) Put(k string, bb []byte) (err error)           { return }
func (this dummydbi) PutObject(k string, o interface{}) (err error) { return }
func (this dummydbi) Get(k string) (bb []byte, err error)           { return }
func (this dummydbi) GetObject(k string, o interface{}) (err error) { return }
func (this dummydbi) Kind() string                                  { return "" }
func (this dummydbi) Context() context.Context                      { return this.c }
func (this dummydbi) NewQuery() (q gae.DbiQuery)                    { return }
func (this dummydbi) Run(gae.DbiQuery) (iter gae.DbiIterator)       { return }
func (this dummydbi) Delete(k string) error                         { return nil }
func (this dummydbi) DeleteAll(ks gae.DbiKeyArr) error              { return nil }

func (this dummydbi) GetRangeKeys(begink, endk string) (arr gae.DbiKeyArr, err error) { return }
func (this dummydbi) GetKeysBeginsWith(begink string) (arr gae.DbiKeyArr, err error)  { return }
func (this dummydbi) GetAll(gae.DbiQuery, interface{}) (arr gae.DbiKeyArr, err error) { return }

func (this dummydbi) IsDone(error) bool { return true }

func (this dummydbi) CreateKey(stringID string, intID int64, parentK *gae.DbiKey) (k gae.DbiKey) {
	return
}

/*
func (this dummydbi) KindProperties() (map[string][]string, error) {
	return nil, nil
}*/

func (this dummydbi) NewContext(c context.Context) gae.DbInt {
	return nil
}

func (this dummydbi) PutAll(ks gae.DbiKeyArr, src interface{}) error {
	return nil
}

func (this dummydbi) RunInTransaction(gae.DbiTransaction, gae.DbiTrxOpts) error {
	return nil
}

func (this dummydbi) NetContext() context.Context {
	return this.c
}

func (this dummydbi) IsCloud() bool {
	return false
}

func init() {
	registry.RegisterDbi(Name, New)
	registry.RegisterDbiEx(Name, NewEx)
}
