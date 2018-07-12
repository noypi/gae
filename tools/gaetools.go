package tools

import (
	"context"

	. "github.com/noypi/gae"
	"github.com/noypi/gae/dbi/std"
	"github.com/noypi/gae/logi/std"
	"github.com/noypi/gae/registry"
	"github.com/noypi/gae/webi/std"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type GaeTools struct {
	Dbi           DbInt
	Webi          WebInt
	Logi          LogInt
	bbJwt         []byte
	appid         string
	kind          string
	ctx           context.Context
	clientStorage *storage.Client
}

func NewTk(ctx context.Context, bbJwt []byte, appid, kind string) (tk *GaeTools, err error) {
	tk = new(GaeTools)
	tk.appid = appid
	tk.kind = kind
	tk.ctx = ctx
	tk.bbJwt = bbJwt
	tk.Logi, err = registry.GetLogi(logi.Name, map[string]interface{}{
		"context": ctx,
	})
	if nil != err {
		return
	}

	dbiExOpts := map[string]interface{}{
		"context": ctx,
		"logger":  tk.Logi,
		"appid":   appid,
		"name":    kind,
	}

	if 0 < len(bbJwt) {
		var tokenSource oauth2.TokenSource
		tokenSource, err = tk.getTokenSource(datastore.ScopeDatastore)
		if nil != err {
			tk.Logi.Errorf("NewTk getTokenSource err=%v", err)
			return
		}
		dbiExOpts["tokenSource"] = tokenSource
	}
	tk.Dbi, err = registry.GetDbiEx(dbi.Name, dbiExOpts)
	if nil != err {
		tk.Logi.Criticalf("NewTk tk.Dbi err=%v", err)
		return
	}
	tk.Webi, err = registry.GetWebi(webi.Name, map[string]interface{}{
		"context": ctx,
		"logger":  tk.Logi,
	})
	if nil != err {
		tk.Logi.Criticalf("NewTk tk.Webi err=%v", err)
		return
	}
	return
}

func (this *GaeTools) getTokenSource(scopes ...string) (oauth2.TokenSource, error) {
	conf, err := google.JWTConfigFromJSON(this.bbJwt, scopes...)
	if err != nil {
		this.Logi.Warningf("GaeTools.getTokenSource err=%v", err)
		return nil, err
	}

	return conf.TokenSource(this.ctx), nil
}

func (this GaeTools) GetWebi() WebInt {
	return this.Webi
}

func (this GaeTools) GetLogi() LogInt {
	return this.Logi
}

func (this GaeTools) GetDbi() DbInt {
	return this.Dbi
}

func (this GaeTools) NetContext() context.Context {
	return this.ctx
}

func (this *GaeTools) StorageClient() *storage.Client {
	if nil == this.clientStorage {
		var err error
		tokenSource, err := this.getTokenSource(storage.ScopeFullControl)
		if nil != err {
			this.Logi.Criticalf("GaeTools.StorageClient err=%v", err)
			return nil
		}
		this.clientStorage, err = storage.NewClient(this.ctx, option.WithTokenSource(tokenSource))
		if nil != err {
			this.Logi.Criticalf("GaeTools.StorageClient err=%v", err)
		}
	}

	return this.clientStorage
}

/*
func (this *GaeTools) StorageContext() context.Context {
	return cloud.WithContext(this.NetContext(), this.appid, this.StorageClient())
}
*/
