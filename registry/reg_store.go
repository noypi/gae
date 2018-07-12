package registry

import (
	"fmt"

	"github.com/noypi/gae"
)

type StoriConstructor func(params map[string]interface{}) (gae.StorInt, error)

var g_storregistry = map[string]StoriConstructor{}

func RegisterStori(name string, constructor StoriConstructor) {
	g_storregistry[name] = constructor
}

func GetStori(name string, params map[string]interface{}) (logi gae.StorInt, err error) {
	if fn, has := g_storregistry[name]; has {
		return fn(params)
	} else {
		err = fmt.Errorf("GetStori name not found=%s", name)
	}
	return
}
