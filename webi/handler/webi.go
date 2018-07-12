package webih

import (
	"context"
	"net/http"

	"github.com/noypi/gae"
	"github.com/noypi/gae/registry"
	"github.com/noypi/router"
)

type _urlfetchClientKeyName int

const (
	UrlFetchClientKeyName _urlfetchClientKeyName = iota
	GAEWebi
)

func GetUrlFetchClient(ctx context.Context) *http.Client {
	c := ctx.(gae.Store)
	if o, exists := c.Get(UrlFetchClientKeyName); exists {
		return o.(*http.Client)
	}
	return nil
}

func GetGAEWebi(ctx context.Context) gae.WebInt {
	c := ctx.(gae.Store)
	if o, exists := c.Get(GAEWebi); exists {
		return o.(gae.WebInt)
	}
	return nil
}

// Ensures http.Client is added
func AddGAEUrlFetch(webiname string, loggerKey, gaectxKey, jarKey interface{}) http.HandlerFunc {
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

		ologger, hasLogger := c.Get(loggerKey)
		if !hasLogger {
			ERR := router.GetErrLog(c)
			ERR.PrintStackTrace(5)
			ERR("AddGAEUrlFetch: failed to get gae.GaeLogger, loggerKey=%v", loggerKey)
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		config := map[string]interface{}{
			gae.WebiContext: appenineCtx,
			gae.WebiLogger:  ologger.(gae.LogInt),
		}

		if jar, exists := c.Get(jarKey); exists {
			config[gae.WebiJar] = jar
		}

		o, err := registry.GetWebi(webiname, config)

		if nil != err {
			ERR := router.GetErrLog(c)
			ERR.PrintStackTrace(5)
			ERR("AddGAEUrlFetch: failed to create new gae.WebInt, err=%v", err)
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		c.Set(UrlFetchClientKeyName, o.Client())
		c.Set(GAEWebi, o)
	}

}
