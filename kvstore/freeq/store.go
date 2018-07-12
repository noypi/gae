package freeq

import (
	"fmt"

	"github.com/noypi/gae"
	"github.com/noypi/kv"
)

type Store struct {
	mo     kv.MergeOperator
	dbi    gae.DbExInt
	logger gae.LogInt
	ns     []byte
}

func New(mo kv.MergeOperator, logger gae.LogInt, dbi gae.DbExInt, namespace []byte) (kv.KVStore, error) {
	if nil == mo {
		mo = &dummymergeop{}
	}
	if nil == dbi {
		return nil, fmt.Errorf("invalid parameter, dbi is nil")
	}
	if nil == logger {
		return nil, fmt.Errorf("invalid parameter, logger is nil")
	}
	if 0 == len(namespace) {
		return nil, fmt.Errorf("invalid parameter, namespace is empty")
	}
	return &Store{
		mo:     mo,
		logger: logger,
		dbi:    dbi,
		ns:     namespace,
	}, nil
}

func (this *Store) Writer() (kv.KVWriter, error) {
	return &Writer{
		ns:     this.ns,
		logger: this.logger,
		store:  this,
	}, nil
}

func (this *Store) Reader() (kv.KVReader, error) {
	return &Reader{
		ns:    this.ns,
		store: this,
	}, nil

}

func (this *Store) Close() error {
	return nil
}

type dummymergeop struct{}

func (this dummymergeop) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	return []byte{}, true
}

func (this dummymergeop) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return []byte{}, true
}

func (this dummymergeop) Name() string {
	return "dummy-mergeop"
}
