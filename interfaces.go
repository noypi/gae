package gae

import (
	"io"
	"time"

	"net/http"
	"net/url"

	"context"

	cloudds "cloud.google.com/go/datastore"
	netcontext "golang.org/x/net/context"
	localds "google.golang.org/appengine/datastore"
)

var (
	LocationPH, _ = time.LoadLocation("Asia/Manila")
)

type Tk interface {
	GetWebi() WebInt
	GetLogi() LogInt
	GetDbi() DbInt
	NetContext() context.Context
}

type DbiKey struct {
	Cloud *cloudds.Key
	Local *localds.Key
}

type DbiKeyArr struct {
	Cloud []*cloudds.Key
	Local []*localds.Key
}

type DbiQuery struct {
	Cloud *cloudds.Query
	Local *localds.Query
	C     context.Context
}

type DbiIterator struct {
	Cloud *cloudds.Iterator
	Local *localds.Iterator
}

type DbiTransaction struct {
	Cloud func(*cloudds.Transaction) error
	Local func(netcontext.Context) error
}

type DbiTrxOpts struct {
	Cloud []cloudds.TransactionOption
	Local *localds.TransactionOptions
}

type DbInt interface {
	Put(k string, bb []byte) (err error)
	Get(k string) (bb []byte, err error)
	Delete(k string) error
}

type DbExInt interface {
	DbInt

	IsCloud() bool

	PutObject(k string, o interface{}) (err error)
	GetObject(k string, o interface{}) (err error)

	//-
	IsDone(error) bool
	Kind() string
	Context() context.Context
	//KindProperties() (map[string][]string, error)
	//-
	DeleteAll(ks DbiKeyArr) error
	PutAll(ks DbiKeyArr, src interface{}) error
	RunInTransaction(fn DbiTransaction, opts DbiTrxOpts) error
	CreateKey(stringID string, intID int64, parentK *DbiKey) DbiKey

	// queries
	GetRangeKeys(begink, endk string) (DbiKeyArr, error)
	GetKeysBeginsWith(begink string) (DbiKeyArr, error)
	NewQuery() DbiQuery
	Run(DbiQuery) DbiIterator
	GetAll(DbiQuery, interface{}) (DbiKeyArr, error)

	// new context
	NewContext(c context.Context) DbInt
}

type LogInt interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Criticalf(format string, args ...interface{})
	Email(subject, body string)
	EmailTo(ss []string, subject, body string, bHtml bool)
	SetCallDepth(n int)
	Output(calldepth int, logFn func(format string, args ...interface{}), s string)
}

type WebInt interface {
	Get(url string, v *url.Values) ([]byte, error)
	Post(url string, v *url.Values) ([]byte, error)
	Client() *http.Client
}

type StorInt interface {
	Delete(fpath string) error
	Reader(fpath string) (io.ReadCloser, error)
	Writer(fpath string) (io.WriteCloser, error)
}
