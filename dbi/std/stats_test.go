package dbi

import (
	"github.com/noypi/gae/logi/std"
	"github.com/noypi/gae/registry"

	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"

	. "gopkg.in/check.v1"
)

func (suite *MySuite) TestWriteKey(c *C) {
	ctx, fnclose, err := aetest.NewContext()
	c.Assert(err, IsNil)
	defer fnclose()

	logger, err := registry.GetLogi(logi.Name, map[string]interface{}{
		"context": ctx,
	})
	c.Assert(err, IsNil)

	db1, err := registry.GetDbiEx(Name, map[string]interface{}{
		"logger":  logger,
		"context": ctx,
		"appid":   appengine.AppID(ctx),
		"name":    "somekind",
	})
	c.Assert(db1.Put("someky", []byte("somebyte")), IsNil)

	/*stats, err := db1.KindProperties()
	c.Assert(err, IsNil)

	logger.Debugf("stats=%v", stats)
	pretty.Println(stats)*/

}
