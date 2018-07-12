package registry

import (
	"fmt"

	"github.com/noypi/gae"
)

type LogiConstructor func(params map[string]interface{}) (gae.LogInt, error)

var g_logregistry = map[string]LogiConstructor{}

func RegisterLogi(name string, constructor LogiConstructor) {
	g_logregistry[name] = constructor
}

func GetLogi(name string, params map[string]interface{}) (logi gae.LogInt, err error) {
	if fn, has := g_logregistry[name]; has {
		return fn(params)
	} else {
		err = fmt.Errorf("GetLogi name not found=%s", name)
	}
	return
}
