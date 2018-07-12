package dbih

import (
	"context"
	"net/http"

	"github.com/noypi/gae"
	logih "github.com/noypi/gae/logi/handler"
	"github.com/noypi/gae/registry"
	"cloud.google.com/go/datastore"
	"github.com/noypi/router"
	"google.golang.org/api/option"
)

type _gaedbiKey int

const (
	GAEDbiKey _gaedbiKey = iota
)

func GetGAEDbi(ctx context.Context) gae.DbExInt {
	c := ctx.(gae.Store)
	if o, exists := c.Get(GAEDbiKey); exists {
		return o.(gae.DbExInt)
	}
	return nil
}

func AddGAEDbi(dbiname, appid, kind, jwtpath, apikey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := router.ContextW(w)
		ERR := router.GetErrLog(c)
		gaectx := gae.GetGAEContext(c)
		logger := logih.GetGAELogi(c)

		opts := []option.ClientOption{option.WithScopes(datastore.ScopeDatastore)}

		if 0 < len(jwtpath) {
			ts, err := gae.GetTokenSource(gaectx, jwtpath, datastore.ScopeDatastore)
			if nil != err {
				ERR.PrintStackTrace(5)
				ERR.Ln("AddGAEDbi: failed to create TokenSource, err=", err.Error())
				c.AbortWithStatus(http.StatusInternalServerError)
				return
			}
			opts = append(opts, option.WithTokenSource(ts))
		}

		if 0 < len(apikey) {
			opts = append(opts, option.WithAPIKey(apikey))
		}

		o, err := registry.GetDbiEx(dbiname, map[string]interface{}{
			gae.DbiContext: gaectx,
			gae.DbiAppID:   appid,
			gae.DbiLogger:  logger,
			gae.DbiName:    kind,
			gae.DbiOpts:    opts,
		})

		if nil != err {
			ERR.PrintStackTrace(5)
			ERR.Ln("AddGAEDbi: failed to create GAE Dba, err=", err.Error())
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		c.Set(GAEDbiKey, o)
	}
}
