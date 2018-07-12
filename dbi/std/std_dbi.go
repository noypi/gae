package dbi

import (
	"fmt"
	"net"
	"time"

	"context"

	"github.com/noypi/gae"
	"github.com/noypi/gae/registry"
	cloudds "cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	localds "google.golang.org/appengine/datastore"
	"google.golang.org/appengine/socket"
	"google.golang.org/grpc"
)

const Name = "standard"

type BlobWithDate struct {
	Bb      []byte    `datastore:",noindex"`
	Updated time.Time `datastore:""`
}
type Blob struct {
	Bb []byte `datastore:",noindex"`
}

type DbAppengine struct {
	c           context.Context
	log         gae.LogInt
	kind        string
	appid       string
	bUseUpdated bool
	bUseCloud   bool
	client      *cloudds.Client
}

var _NilKey = &gae.DbiKey{}

// required params:
//      context.Context
//      logger LogInt
//      appid string
//      name string
func NewEx(params map[string]interface{}) (db gae.DbExInt, err error) {
	c, has := params[gae.DbiContext].(context.Context)
	if !has {
		return nil, fmt.Errorf("invalid params, context is not  defined.")
	}
	logger, has := params[gae.DbiLogger].(gae.LogInt)
	if !has {
		return nil, fmt.Errorf("invalid params, logger is not  defined.")
	}
	appid, has := params[gae.DbiAppID].(string)
	if !has {
		return nil, fmt.Errorf("invalid params, appid is not  defined.")
	}
	name, has := params[gae.DbiName].(string)
	if !has {
		return nil, fmt.Errorf("invalid params, name is not  defined.")
	}

	bUseCloud, _ := params[gae.DbiUseCloud].(bool)

	opts, has := params[gae.DbiOpts].([]option.ClientOption)
	var dbo *DbAppengine
	if has {
		dbo, err = newDbInt(c, logger, appid, name, bUseCloud, opts)
	} else {
		dbo, err = newDbInt(c, logger, appid, name, bUseCloud, nil)
	}
	if nil != err {
		return
	}

	bUpdated, has := params[gae.DbiUseUpdated].(bool)
	if has {
		dbo.bUseUpdated = bUpdated
	}
	dbo.bUseCloud = bUseCloud
	db = dbo

	return
}

func New(params map[string]interface{}) (db gae.DbInt, err error) {
	return NewEx(params)
}

func newDbInt(c context.Context, logger gae.LogInt, appid, name string, bUseCloud bool, opts []option.ClientOption) (db *DbAppengine, err error) {
	db = new(DbAppengine)
	db.kind = name
	db.appid = appid
	db.log = logger
	db.c = c

	if bUseCloud {
		dialer := func(addr string, timeout time.Duration) (net.Conn, error) {
			return socket.DialTimeout(c, "tcp", addr, timeout)
		}
		opts = append(opts, option.WithGRPCDialOption(grpc.WithDialer(dialer)))
		/*
			//-------------- test
			// when dialer is a socket, an error occurred asking to enable billing
			// test dialer, when timeout errors are occuring, or Error code 123
			conn, err := dialer("datastore.googleapis.com:443", 5*time.Second)
			if err != nil {
				logger.Errorf("Dial: %v", err)
				return
			}
			logger.Infof("Addr: %v\n", conn.RemoteAddr())
			conn.Close()
			//-------------------
		*/

		db.client, err = cloudds.NewClient(c, appid, opts...)
	}

	if nil != logger && nil != err {
		logger.Errorf("newDbInt() datastore.NewClient err=%v", err)
	}
	return
}

func (this DbAppengine) Kind() string {
	return this.kind
}

func (this DbAppengine) IsDone(err error) bool {
	return iterator.Done == err
}

func (this DbAppengine) CreateKey(stringID string, intID int64, parentK *gae.DbiKey) (k gae.DbiKey) {
	if nil == parentK {
		parentK = _NilKey
	}
	if this.bUseCloud {
		if 0 < len(stringID) {
			k.Cloud = cloudds.NameKey(this.kind, stringID, parentK.Cloud)
		} else if 0 < intID {
			k.Cloud = cloudds.IDKey(this.kind, intID, parentK.Cloud)
		}
		k.Cloud = cloudds.IncompleteKey(this.kind, parentK.Cloud)
	} else {
		k.Local = localds.NewKey(this.c, this.kind, stringID, intID, parentK.Local)
	}

	return
}

func (this DbAppengine) Context() context.Context {
	return this.c
}

func (this *DbAppengine) Put(k string, bb []byte) (err error) {
	//this.log.Debugf("DbAppengine Put kind=%s, k=%s", this.kind, k)
	var blob interface{}
	if this.bUseUpdated {
		blob = &BlobWithDate{
			Updated: time.Now().In(gae.LocationPH),
			Bb:      bb,
		}
	} else {
		blob = &Blob{
			Bb: bb,
		}
	}

	return this.PutObject(k, blob)
}

func (this DbAppengine) Get(k string) (bb []byte, err error) {
	//this.log.Debugf("DbAppengine Get k=%s", k)
	blob := new(Blob)
	if err = this.GetObject(k, blob); nil != err {
		return
	}
	bb = blob.Bb
	return
}

func (this *DbAppengine) PutObject(k string, o interface{}) (err error) {
	dbk := this.CreateKey(k, 0, _NilKey)
	if this.bUseCloud {
		_, err = this.client.Put(this.c, dbk.Cloud, o)
	} else {
		_, err = localds.Put(this.c, dbk.Local, o)
	}
	return
}

func (this DbAppengine) GetObject(k string, o interface{}) (err error) {
	dbk := this.CreateKey(k, 0, _NilKey)
	if this.bUseCloud {
		err = this.client.Get(this.c, dbk.Cloud, o)
	} else {
		err = localds.Get(this.c, dbk.Local, o)
	}
	return
}

func (this DbAppengine) NewQuery() (q gae.DbiQuery) {
	if this.bUseCloud {
		q.Cloud = cloudds.NewQuery(this.kind)
	} else {
		q.Local = localds.NewQuery(this.kind)
	}
	q.C = this.c

	return
}

func (this *DbAppengine) Run(q gae.DbiQuery) (iter gae.DbiIterator) {
	if this.bUseCloud {
		iter.Cloud = this.client.Run(this.c, q.Cloud)
	} else {
		iter.Local = q.Local.Run(this.c)
	}

	return
}

func (this *DbAppengine) GetAll(q gae.DbiQuery, dst interface{}) (arr gae.DbiKeyArr, err error) {
	if this.bUseCloud {
		arr.Cloud, err = this.client.GetAll(this.c, q.Cloud, dst)
	} else {
		arr.Local, err = q.Local.GetAll(this.c, dst)
	}

	return
}

func (this *DbAppengine) GetRangeKeys(begin, end string) (ks gae.DbiKeyArr, err error) {
	beginK := this.CreateKey(begin, 0, _NilKey)
	endK := this.CreateKey(end, 0, _NilKey)

	//this.log.Debugf("DbAppengine GetRangeKeys kind=%s, begin=%s", this.kind, begin)
	//this.log.Debugf("DbAppengine GetRangeKeys end=%s", end)
	q := this.NewQuery()
	if this.bUseCloud {
		q0 := q.Cloud.KeysOnly().Filter("__key__ >=", beginK.Cloud).Filter("__key__ <=", endK.Cloud)
		ks.Cloud, err = this.client.GetAll(this.c, q0, nil)
	} else {
		q0 := q.Local.KeysOnly().Filter("__key__ >=", beginK.Local).Filter("__key__ <=", endK.Local)
		ks.Local, err = q0.GetAll(this.c, nil)
	}

	return
}

func (this *DbAppengine) GetKeysBeginsWith(begink string) (ks gae.DbiKeyArr, err error) {
	return this.GetRangeKeys(begink+"\x00\x00\x00\x00", begink+"\xFF\xFF\xFF\xFF")
}

func (this *DbAppengine) Delete(key string) (err error) {
	k := this.CreateKey(key, 0, _NilKey)
	if this.bUseCloud {
		this.client.Delete(this.c, k.Cloud)
	} else {
		localds.Delete(this.c, k.Local)
	}
	return
}

func (this *DbAppengine) DeleteAll(ks gae.DbiKeyArr) error {
	if this.bUseCloud {
		return this.client.DeleteMulti(this.c, ks.Cloud)
	}
	return localds.DeleteMulti(this.c, ks.Local)
}

func (this *DbAppengine) PutAll(ks gae.DbiKeyArr, src interface{}) (err error) {
	if this.bUseCloud {
		_, err = this.client.PutMulti(this.c, ks.Cloud, src)
	} else {
		_, err = localds.PutMulti(this.c, ks.Local, src)
	}
	return
}

func (this *DbAppengine) RunInTransaction(fn gae.DbiTransaction, opts gae.DbiTrxOpts) (err error) {
	//var opts = datastore.TransactionOption{}
	//opts.XG = true
	if this.bUseCloud {
		_, err = this.client.RunInTransaction(this.c, fn.Cloud, opts.Cloud...)
	} else {
		err = localds.RunInTransaction(this.c, fn.Local, opts.Local)
	}

	return err
}

func (this *DbAppengine) NewContext(c context.Context) gae.DbInt {
	db := new(DbAppengine)
	db.appid = this.appid
	db.c = c
	db.kind = this.kind
	db.log = this.log
	return db
}

func (this *DbAppengine) IsCloud() bool {
	return this.bUseCloud
}

func init() {
	registry.RegisterDbi(Name, New)
	registry.RegisterDbiEx(Name, NewEx)
}
