package stori

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/noypi/gae"
	"github.com/noypi/gae/registry"
	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
	"google.golang.org/appengine/file"
)

const Name = "standard"

type Stori struct {
	client    *storage.Client
	c         context.Context
	logger    gae.LogInt
	projectID string
	bucket    string
}

func New(params map[string]interface{}) (gae.StorInt, error) {
	c, has := params[gae.StoriContext].(context.Context)
	if !has {
		return nil, fmt.Errorf("Error: invalid params, context is needed")
	}
	logger, has := params[gae.StoriLogger].(gae.LogInt)
	if !has {
		return nil, fmt.Errorf("Error: invalid params, logger is needed")
	}
	projectID, _ := params[gae.StoriProjectID].(string)
	/*if !has || 0 == len(projectID) {
		return nil, fmt.Errorf("Error: invalid params, projectID is needed")
	}*/

	var err error
	bucket, _ := params[gae.StoriBucket].(string)
	if 0 == len(bucket) {
		if bucket, err = file.DefaultBucketName(c); nil != err {
			return nil, err
		}
	}

	opts, _ := params[gae.StoriOpts].([]option.ClientOption)
	return newStori(c, logger, projectID, bucket, opts)
}

func newStori(c context.Context, logger gae.LogInt, projectID, bucket string, opts []option.ClientOption) (gae.StorInt, error) {
	o := new(Stori)
	o.c = c
	o.logger = logger
	o.projectID = projectID
	o.bucket = bucket
	var err error
	if 0 == len(opts) {
		o.client, err = storage.NewClient(c)
	} else {
		o.client, err = storage.NewClient(c, opts...)
	}
	if nil != err {
		return nil, err
	}
	return o, nil
}

func (this *Stori) ensureBucketExist() (*storage.BucketHandle, error) {
	bkt := this.client.Bucket(this.bucket)
	if _, err := bkt.Attrs(this.c); err == storage.ErrBucketNotExist {
		if err := this.client.Bucket(this.bucket).Create(this.c, this.projectID, nil); nil != err {
			return nil, err
		}
	}
	return bkt, nil
}

func (this *Stori) Create(fpath string) (io.WriteCloser, error) {
	bkt, err := this.ensureBucketExist()
	if nil != err {
		return nil, err
	}

	fpath = filepath.Clean(fpath)
	obj := bkt.Object(fpath)

	return obj.NewWriter(this.c), nil
}

func (this *Stori) Reader(fpath string) (io.ReadCloser, error) {
	bkt, err := this.ensureBucketExist()
	if nil != err {
		return nil, err
	}

	fpath = filepath.Clean(fpath)
	obj := bkt.Object(fpath)
	if _, err := obj.Attrs(this.c); nil != err {
		return nil, err
	}

	return obj.NewReader(this.c)
}

func (this *Stori) Writer(fpath string) (io.WriteCloser, error) {
	bkt, err := this.ensureBucketExist()
	if nil != err {
		return nil, err
	}

	fpath = filepath.Clean(fpath)
	obj := bkt.Object(fpath)

	return obj.NewWriter(this.c), nil
}

func (this *Stori) Delete(fpath string) error {
	bkt, err := this.ensureBucketExist()
	if nil != err {
		return err
	}

	fpath = filepath.Clean(fpath)
	obj := bkt.Object(fpath)

	return obj.Delete(this.c)

}

func init() {
	registry.RegisterStori(Name, New)
}
