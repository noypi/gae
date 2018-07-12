package freeq

import (
	"context"

	"github.com/noypi/gae/dbi/std"
	"github.com/noypi/gae/logi/std"
	"github.com/noypi/gae/registry"
	"github.com/noypi/kv"
	"google.golang.org/appengine"
)

func GetDefault(ctx context.Context, kind, ns string) (store kv.KVStore, err error) {
	logger, err := registry.GetLogi(logi.Name, map[string]interface{}{
		"context": ctx,
	})
	if nil != err {
		return
	}

	dbint, err := registry.GetDbiEx(dbi.Name, map[string]interface{}{
		"logger":  logger,
		"context": ctx,
		"appid":   appengine.AppID(ctx),
		"name":    kind,
	})
	if nil != err {
		return
	}

	store, err = New(dummymergeop{}, logger, dbint, []byte(ns))
	return
}
