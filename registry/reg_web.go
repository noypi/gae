package registry

import (
	"fmt"

	"github.com/noypi/gae"
)

type WebiConstructor func(params map[string]interface{}) (gae.WebInt, error)

var g_webregistry = map[string]WebiConstructor{}

func RegisterWebi(name string, constructor WebiConstructor) {
	g_webregistry[name] = constructor
}

func GetWebi(name string, params map[string]interface{}) (logi gae.WebInt, err error) {
	if fn, has := g_webregistry[name]; has {
		return fn(params)
	} else {
		err = fmt.Errorf("GetWebi name not found=%s", name)
	}
	return
}
