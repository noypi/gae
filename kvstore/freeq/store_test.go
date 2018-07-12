package freeq

import (
	"fmt"

	"github.com/noypi/gae/dbi/std"
	"github.com/noypi/gae/logi/std"
	"github.com/noypi/gae/registry"

	"context"

	"github.com/noypi/gae"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"

	"github.com/noypi/kv"

	. "gopkg.in/check.v1"
)

func NewContext() (context.Context, func(), error) {
	inst, err := aetest.NewInstance(&aetest.Options{AppID: "", StronglyConsistentDatastore: true})
	if err != nil {
		return nil, nil, err
	}
	req, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		inst.Close()
		return nil, nil, err
	}
	ctx := appengine.NewContext(req)
	return ctx, func() {
		inst.Close()
	}, nil
}

func (suite *MySuite) TestWriter(c *C) {
	store, db, logger, fncloser := getStore(c, "mynamespace")
	defer fncloser()

	wrtr, err := store.Writer()
	c.Assert(err, IsNil)

	batch := wrtr.NewBatch()
	c.Assert(batch, NotNil)

	var testdata = []string{
		"some321value1 2016/04/24 23:07:06 DEBUG: [github.com/noypi/gae/kvstore/freeq] (store_test.go:(*MySuite).TestWriter:6 INFO     2016-04-24 15:08:19,062 api_server.py:205] Starting API server at: http://localhost:36365 ARNING  2016-04-24 15:08:11,050 simple_search_stub.py:1126] Could not read search indexes from /tmp/appengine.testapp.davidking/s",
		"some123value2",
		"some654value3",
		"some456value4 2016/04/24 23:07:06 DEBUG: [github.com/noypi/gae/kvstore/freeq] (store_test.go:(*MySuite).TestWriter:6",
		"status_graph",
		"C",
	}

	for i, vv := range testdata {
		vvbb := []byte(vv)
		batch.Set([]byte(fmt.Sprintf("%s-%.2d", "mykey", i)), vvbb)
	}
	err = wrtr.ExecuteBatch(batch)
	c.Assert(err, IsNil)

	logger.Debugf("------------------------------")
	ks, err := db.GetAll(db.NewQuery().KeysOnly(), nil)
	c.Assert(err, IsNil)
	logger.Debugf("ks len=%d", len(ks.Local))
	for _, k := range ks.Local {
		logger.Debugf("k name=%s", k.StringID())
	}

	// read results
	rdr, err := store.Reader()
	c.Assert(err, IsNil)

	for i, vv := range testdata {
		bb, err := rdr.Get([]byte(fmt.Sprintf("%s-%.2d", "mykey", i)))
		c.Assert(err, IsNil)

		vvbb := []byte(vv)
		c.Assert(bb, DeepEquals, vvbb)
		//c.Assert(bytes.Compare(bb, vvbb), Equals, 0)
	}

}

func (suite *MySuite) TestReplaceValue(c *C) {
	store, _, _, fncloser := getStore(c, "mynamespace")
	defer fncloser()

	wrtr, err := store.Writer()
	c.Assert(err, IsNil)

	var testdata = []string{
		"some321value1 2016/04/24 23:07:06 DEBUG: [github.com/noypi/gae/kvstore/freeq] (store_test.go:(*MySuite).TestWriter:6 INFO     2016-04-24 15:08:19,062 api_server.py:205] Starting API server at: http://localhost:36365 ARNING  2016-04-24 15:08:11,050 simple_search_stub.py:1126] Could not read search indexes from /tmp/appengine.testapp.davidking/s 23:07:06 DEBUG: [github.com/noypi/gae/kvstore/freeq] (store_test.go:(*MySuite).TestWriter:6 INFO     2016-04-24 15:08:19,062 api_server.py:205] Starting API server at: http://localhost:36365 ARNING  2016-04-24 15:08:11,050 simple_search_stub.py:1126] Could not read search indexes from /tmp/appengine.testapp.davidking/s",
		"some123value2",
		"some654value3",
		"some456value4 2016/04/24 23:07:06 D",
	}

	// read results
	rdr, err := store.Reader()
	c.Assert(err, IsNil)

	for _, vv := range testdata {
		vvbb := []byte(vv)
		/*// display current
		logger.Debugf("!! BEFORE begin execute batch--------------")
		ks, err := rdr.(*Reader).store.dbi.GetAll(rdr.(*Reader).store.dbi.NewQuery().KeysOnly(), nil)
		c.Assert(err, IsNil)
		for _, k := range ks {
			logger.Debugf("k name=%s", k.StringID())
		}
		logger.Debugf("!! BEFORE end------------------------------")*/

		// write
		batch := wrtr.NewBatch()
		c.Assert(batch, NotNil)
		batch.Set([]byte("samekey"), vvbb)
		err = wrtr.ExecuteBatch(batch)
		c.Assert(err, IsNil)
		batch.Close()

		// read
		bb, err := rdr.Get([]byte("samekey"))
		c.Assert(err, IsNil)
		c.Assert(bb, DeepEquals, vvbb)

	}

}

func (suite *MySuite) TestReplaceValueInBatch(c *C) {
	store, _, _, fncloser := getStore(c, "mynamespace")
	defer fncloser()

	wrtr, err := store.Writer()
	c.Assert(err, IsNil)

	var testdata = []string{
		"some321value1 2016/04/24 23:07:06 DEBUG: [github.com/noypi/gae/kvstore/freeq] (store_test.go:(*MySuite).TestWriter:6 INFO     2016-04-24 15:08:19,062 api_server.py:205] Starting API server at: http://localhost:36365 ARNING  2016-04-24 15:08:11,050 simple_search_stub.py:1126] Could not read search indexes from /tmp/appengine.testapp.davidking/s",
		"some123value2",
		"some654value3",
		"some456value4 2016/04/24 23:07:06 D",
	}

	batch := wrtr.NewBatch()
	c.Assert(batch, NotNil)
	for _, vv := range testdata {
		vvbb := []byte(vv)

		// write
		batch.Set([]byte("samekey"), vvbb)
		err = wrtr.ExecuteBatch(batch)
		c.Assert(err, IsNil)

	}

	// read results
	rdr, err := store.Reader()
	c.Assert(err, IsNil)

	/*// display current
	logger.Debugf("!! BEFORE begin execute batch--------------")
	ks, err := rdr.(*Reader).store.dbi.GetAll(rdr.(*Reader).store.dbi.NewQuery().KeysOnly(), nil)
	c.Assert(err, IsNil)
	for _, k := range ks {
		logger.Debugf("k name=%s", k.StringID())
	}
	logger.Debugf("!! BEFORE end------------------------------")*/

	// read
	bb, err := rdr.Get([]byte("samekey"))
	c.Assert(err, IsNil)
	c.Assert(bb, DeepEquals, []byte(testdata[len(testdata)-1]))

}

func (suite *MySuite) TestPrefixIterator(c *C) {
	store, _, logger, fncloser := getStore(c, "mynamespace")
	defer fncloser()

	wrtr, err := store.Writer()
	c.Assert(err, IsNil)

	var testdata = []string{
		"some0321",
		"some1123value2",
		"some2654value3",
		"some3456value4 2016/04/24 23:07:06 D",
	}

	var testdataNoise = []string{
		"s0m30321",
		"s0m31123value2",
		"s0m32654value3",
		"s0m33456value4 2016/04/24 23:07:06 D",
		"spm32654value3",
	}

	batch := wrtr.NewBatch()
	c.Assert(batch, NotNil)
	for _, vv := range testdata {
		vvbb := []byte(vv)
		batch.Set(vvbb, []byte("val-"+vv))
	}
	for _, vv := range testdataNoise {
		vvbb := []byte(vv)
		batch.Set(vvbb, []byte("val-"+vv))
	}
	err = wrtr.ExecuteBatch(batch)
	c.Assert(err, IsNil)

	// read results
	rdr, err := store.Reader()
	c.Assert(err, IsNil)
	it := rdr.PrefixIterator([]byte("some"))
	c.Assert(it.Count(), Equals, len(testdata))

	for j := 0; j < 2; j++ {
		for i, vv := range testdata {
			if i < len(testdata) {
				iter := it.(*Iterator)
				logger.Debugf("error=%v, reverse=%v", iter.currerr, iter.reverse)
				c.Assert(it.Valid(), Equals, true)
			}

			logger.Debugf("vv=%s", vv)
			c.Assert(it.Key(), DeepEquals, []byte(vv))
			c.Assert(it.Value(), DeepEquals, []byte("val-"+vv))
			it.Next()
		}
		if j == 0 {
			it.Reset()
		}
	}

	it.Next()

	logger.Debugf("it.Key()=%s", string(it.Key()))
	c.Assert(it.Key(), IsNil)
	c.Assert(it.Value(), IsNil)
	c.Assert(it.Valid(), Equals, false)

	logger.Debugf("----------------------------------- doing reverse")

	// reverse test
	it = rdr.ReversePrefixIterator([]byte("some"))
	c.Assert(it.Count(), Equals, len(testdata))

	for j := 0; j < 2; j++ {
		for i := len(testdata) - 1; i >= 0; i-- {
			var vv = testdata[i]
			if i < len(testdata) {
				iter := it.(*Iterator)
				logger.Debugf("error=%v, reverse=%v", iter.currerr, iter.reverse)
				c.Assert(it.Valid(), Equals, true)
			}

			logger.Debugf("vv=%s", vv)
			c.Assert(it.Key(), DeepEquals, []byte(vv))
			c.Assert(it.Value(), DeepEquals, []byte("val-"+vv))
			it.Next()
		}
		if j == 0 {
			it.Reset()
		}
	}

	it.Next()

	logger.Debugf("it.Key()=%s", string(it.Key()))
	c.Assert(it.Key(), IsNil)
	c.Assert(it.Value(), IsNil)
	c.Assert(it.Valid(), Equals, false)

}

func (suite *MySuite) TestRangeIterator(c *C) {
	store, _, logger, fncloser := getStore(c, "mynamespace")
	defer fncloser()

	wrtr, err := store.Writer()
	c.Assert(err, IsNil)

	var testdata = []string{
		"some2654value3",
		"some3456value4 2016/04/24 23:07:06 D",
		"some4563456value4 2016/04/24 23:07:06 D",
	}

	var testdataNoise = []string{
		"some0321",
		"some1123value2",
		"some46123",
		"some7894563456value4 2016/04/24 23:07:06 D",
		"spm30321",
		"s0m30321",
		"s0m31123value2",
		"s0m32654value3",
		"s0m33456value4 2016/04/24 23:07:06 D",
	}

	batch := wrtr.NewBatch()
	c.Assert(batch, NotNil)
	for _, vv := range testdata {
		vvbb := []byte(vv)
		batch.Set(vvbb, []byte("val-"+vv))
	}
	for _, vv := range testdataNoise {
		vvbb := []byte(vv)
		batch.Set(vvbb, []byte("val-"+vv))
	}
	err = wrtr.ExecuteBatch(batch)
	c.Assert(err, IsNil)

	// read results
	rdr, err := store.Reader()
	c.Assert(err, IsNil)
	it := rdr.RangeIterator([]byte("some2"), []byte("some45"))
	c.Assert(it.Count(), Equals, len(testdata))

	for j := 0; j < 2; j++ {
		logger.Debugf("loop j=%d", j)
		for i, vv := range testdata {
			if i < len(testdata) {
				iter := it.(*Iterator)
				logger.Debugf("error=%v, reverse=%v", iter.currerr, iter.reverse)
				c.Assert(it.Valid(), Equals, true)
			}

			logger.Debugf("vv=%s", vv)
			c.Assert(it.Key(), DeepEquals, []byte(vv))
			c.Assert(it.Value(), DeepEquals, []byte("val-"+vv))
			it.Next()
		}
		if j == 0 {
			it.Reset()
		}
	}

	it.Next()

	logger.Debugf("it.Key()=%s", string(it.Key()))
	c.Assert(it.Key(), IsNil)
	c.Assert(it.Value(), IsNil)
	c.Assert(it.Valid(), Equals, false)

	// read reverse range
	it = rdr.ReverseRangeIterator([]byte("some2"), []byte("some45"))
	c.Assert(it.Count(), Equals, len(testdata))

	for j := 0; j < 2; j++ {
		for i := len(testdata) - 1; i >= 0; i-- {
			var vv = testdata[i]
			if i < len(testdata) {
				iter := it.(*Iterator)
				logger.Debugf("error=%v, reverse=%v", iter.currerr, iter.reverse)
				c.Assert(it.Valid(), Equals, true)
			}

			logger.Debugf("vv=%s", vv)
			c.Assert(it.Key(), DeepEquals, []byte(vv))
			c.Assert(it.Value(), DeepEquals, []byte("val-"+vv))
			it.Next()
		}
		if j == 0 {
			it.Reset()
		}
	}

	it.Next()

	logger.Debugf("it.Key()=%s", string(it.Key()))
	c.Assert(it.Key(), IsNil)
	c.Assert(it.Value(), IsNil)
	c.Assert(it.Valid(), Equals, false)

}

func (suite *MySuite) TestDelete(c *C) {
	store, _, logger, fncloser := getStore(c, "mynamespace")
	defer fncloser()

	wrtr, err := store.Writer()
	c.Assert(err, IsNil)

	var testdata = []string{
		"some2654value3",
		"some3456value4",
		"some3456value4 2016/04/24 23:07:06 D",
	}
	var afterdelTestdata = []string{
		"some2654value3",
		"some3456value4 2016/04/24 23:07:06 D",
	}

	// insert
	batch := wrtr.NewBatch()
	c.Assert(batch, NotNil)
	for _, vv := range testdata {
		vvbb := []byte(vv)
		batch.Set(vvbb, []byte("val-"+vv))
	}
	err = wrtr.ExecuteBatch(batch)
	c.Assert(err, IsNil)

	// delete
	batch = wrtr.NewBatch()
	c.Assert(batch, NotNil)
	logger.Debugf("deleting=%s", testdata[1])
	batch.Delete([]byte(testdata[1]))
	err = wrtr.ExecuteBatch(batch)
	c.Assert(err, IsNil)
	logger.Debugf("done deleting.")

	// read results
	rdr, err := store.Reader()
	c.Assert(err, IsNil)
	bb, err := rdr.Get([]byte(testdata[1]))
	logger.Debugf("bb=%s", string(bb))
	c.Assert(err, IsNil)
	c.Assert(bb, IsNil)

	it := rdr.RangeIterator([]byte("some2"), []byte("some34"))

	for i, vv := range afterdelTestdata {
		if i < len(testdata) {
			c.Assert(it.Valid(), Equals, true)
		}

		logger.Debugf("vv=%s", vv)
		c.Assert(it.Key(), DeepEquals, []byte(vv))
		c.Assert(it.Value(), DeepEquals, []byte("val-"+vv))
		it.Next()
	}

	it.Next()

	logger.Debugf("it.Key()=%s", string(it.Key()))
	c.Assert(it.Key(), IsNil)
	c.Assert(it.Value(), IsNil)
	c.Assert(it.Valid(), Equals, false)

}

func getStore(c *C, namespace string) (store kv.KVStore, db gae.DbExInt, logger gae.LogInt, fncloser func()) {
	var err error
	var ctx context.Context
	//&aetest.Options{"", true}
	ctx, fncloser, err = NewContext()
	c.Assert(err, IsNil)

	logger, err = registry.GetLogi(logi.Name, map[string]interface{}{
		"context": ctx,
	})
	c.Assert(err, IsNil)

	db, err = registry.GetDbiEx(dbi.Name, map[string]interface{}{
		"logger":  logger,
		"context": ctx,
		"appid":   appengine.AppID(ctx),
		"name":    "mytestdb",
	})
	c.Assert(err, IsNil)

	store, err = New(dummymergeop{}, logger, db, []byte(namespace))
	c.Assert(err, IsNil)
	return
}

func (suite *MySuite) TestMultiGet(c *C) {
	store, _, _, fncloser := getStore(c, "mynamespace")
	defer fncloser()

	wrtr, err := store.Writer()
	c.Assert(err, IsNil)

	batch := wrtr.NewBatch()
	c.Assert(batch, NotNil)

	var testdata = []string{
		"some321value1",
		"some123value2",
		"some654value3",
		"some456value4",
	}

	for i, vv := range testdata {
		vvbb := []byte(vv)
		batch.Set([]byte(fmt.Sprintf("%s-%.2d", "mykey", i)), vvbb)
	}
	err = wrtr.ExecuteBatch(batch)
	c.Assert(err, IsNil)

	// read results
	rdr, err := store.Reader()
	c.Assert(err, IsNil)

	var ks [][]byte
	for i, _ := range testdata {
		ks = append(ks, []byte(fmt.Sprintf("%s-%.2d", "mykey", i)))
	}
	res, err := rdr.MultiGet(ks)
	c.Assert(err, IsNil)
	c.Assert(len(res), Equals, len(testdata))

	for i, vvres := range res {
		c.Assert(vvres, DeepEquals, []byte(testdata[i]))
	}

}
