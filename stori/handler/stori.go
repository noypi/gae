package storih

import (
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/noypi/gae"
	logih "github.com/noypi/gae/logi/handler"
	"github.com/noypi/gae/registry"
	"github.com/noypi/router"
	"google.golang.org/api/option"
)

type _gaestoriKey int

const (
	GAEStoriKey _gaestoriKey = iota
)

func GetGAEStori(c router.Store) gae.StorInt {
	if o, exists := c.Get(GAEStoriKey); exists {
		return o.(gae.StorInt)
	}
	return nil
}

func AddGAEStori(storiname, jwtpath string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := router.ContextW(w)
		ERR := router.GetErrLog(c)
		gaectx := gae.GetGAEContext(c)
		logger := logih.GetGAELogi(c)

		ts, err := gae.GetTokenSource(gaectx, jwtpath, storage.ScopeFullControl)
		if nil != err {
			ERR.PrintStackTrace(5)
			ERR.Ln("AddGAEStori: failed to create TokenSource, err=", err.Error())
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		o, err := registry.GetStori(storiname, map[string]interface{}{
			gae.StoriBucket:  "",
			gae.StoriContext: gaectx,
			gae.StoriLogger:  logger,
			gae.StoriOpts: []option.ClientOption{
				option.WithScopes(storage.ScopeFullControl),
				option.WithTokenSource(ts),
			},
		})

		if nil != err {
			ERR.PrintStackTrace(5)
			ERR.Ln("AddGAEStori: failed to create GAE StorageInt, err=", err.Error())
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		c.Set(GAEStoriKey, o)
	}
}
