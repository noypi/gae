package freeq

import (
	"bytes"
	"fmt"

	"github.com/noypi/gae/dbi/std"
	"github.com/noypi/gae/logi/std"
	"github.com/noypi/gae/registry"

	"google.golang.org/appengine"

	. "gopkg.in/check.v1"
)

func (suite *MySuite) TestWriteKey(c *C) {
	//&aetest.Options{"", true}
	ctx, fnclose, err := NewContext()
	c.Assert(err, IsNil)
	defer fnclose()

	c.Assert(err, IsNil)

	logger, err := registry.GetLogi(logi.Name, map[string]interface{}{
		"context": ctx,
	})
	c.Assert(err, IsNil)

	db, err := registry.GetDbiEx(dbi.Name, map[string]interface{}{
		"logger":  logger,
		"context": ctx,
		"appid":   appengine.AppID(ctx),
		"name":    "mytestdb",
	})
	c.Assert(err, IsNil)

	for i, vv := range []string{
		"some321value1", "some123value2",
		"some654value3", "some456value4",
	} {
		vvbb := []byte(vv)
		db.Put(fmt.Sprintf("%s-%.2d", "mykey", i), vvbb)
	}

	ks, err := db.GetAll(db.NewQuery().KeysOnly(), nil)
	c.Assert(err, IsNil)
	logger.Debugf("ks len=%d", len(ks.Local))
	for _, k := range ks.Local {
		logger.Debugf("k name=%s", k.StringID())
	}

	for i, vv := range []string{
		"some321value1", "some123value2",
		"some654value3", "some456value4",
	} {
		bb, err := db.Get(fmt.Sprintf("%s-%.2d", "mykey", i))
		c.Assert(err, IsNil)

		vvbb := []byte(vv)
		c.Assert(bytes.Compare(bb, vvbb), Equals, 0)
	}

}
