package logih

import (
	"context"
	"net/http"

	"github.com/noypi/gae"
	"github.com/noypi/gae/registry"
	"github.com/noypi/router"
	"github.com/noypi/webutil"
)

type _loggerKeyName int

const (
	GAELogi _loggerKeyName = iota
)

func GetGAELogi(ctx context.Context) gae.LogInt {
	c := ctx.(gae.Store)
	if o, exists := c.Get(GAELogi); exists {
		return o.(gae.LogInt)
	}
	return nil
}

func AddGAELogi(loginame string, gaectxKey interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := router.ContextW(w)

		appenineCtx, hasGaectx := c.Get(gaectxKey)
		if !hasGaectx {
			ERR := router.GetErrLog(c)
			ERR.PrintStackTrace(5)
			ERR("AddGAELogi: no GAECtx, key=%v", gaectxKey)
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		o, err := registry.GetLogi(loginame, map[string]interface{}{
			"context": appenineCtx,
		})

		if nil != err {
			ERR := router.GetErrLog(c)
			ERR.PrintStackTrace(5)
			ERR("AddGAELogi: failed to create new gae.Logi, err=%v", err)
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		o.SetCallDepth(6)
		webutil.WithErrLogger(c, o.Errorf)
		webutil.WithInfoLogger(c, o.Infof)
		webutil.WithWarnLogger(c, o.Warningf)
		webutil.WithDebugLogger(c, o.Debugf)

		c.Set(GAELogi, o)
	}

}
