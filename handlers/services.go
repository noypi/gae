package handlers

import (
	"net/http"

	"github.com/noypi/gae"
	dbih "github.com/noypi/gae/dbi/handler"
	"github.com/noypi/gae/dbi/std"
	logih "github.com/noypi/gae/logi/handler"
	"github.com/noypi/gae/logi/std"
	webih "github.com/noypi/gae/webi/handler"
	"github.com/noypi/gae/webi/std"
	"github.com/noypi/webutil"
)

func GetEssentialHandlers() http.HandlerFunc {
	hs := []http.HandlerFunc{
		AddGAEContext,
		logih.AddGAELogi(logi.Name, gae.GAEContextKey),
	}
	return webutil.HttpSequence(nil, hs...)
}

type ServicesOpts struct {
	UseDatastore bool
	UseUrlFetch  bool

	Namespace string
	AppID     string
	JWTPath   string
	APIKey    string

	// enable by default
	DisableDBICache bool
}

// assumes Session exists via:
//       mux.Sstore.AddSessionHandler("..."),
func AddServices(opts ServicesOpts) http.HandlerFunc {
	var hs []http.HandlerFunc

	if opts.UseDatastore {
		hs = append(hs, dbih.AddGAEDbi(dbi.Name, opts.Namespace, opts.AppID, opts.JWTPath, opts.APIKey))
	}

	if opts.UseUrlFetch {
		hs = append(hs, webih.AddGAEUrlFetch(webi.Name, logih.GAELogi, gae.GAEContextKey, webutil.PersistentCookieKey))
	}

	if !opts.DisableDBICache {
		hs = append(hs, webutil.UseDbiCache(opts.Namespace, dbih.GAEDbiKey))
	}

	return webutil.HttpSequence(nil, hs...)
}
