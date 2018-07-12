package registry

import (
	"fmt"

	"github.com/noypi/gae"
)

type DbiConstructor func(params map[string]interface{}) (db gae.DbInt, err error)
type DbiExConstructor func(params map[string]interface{}) (db gae.DbExInt, err error)

var g_dbregistry = map[string]DbiConstructor{}
var g_dbexregistry = map[string]DbiExConstructor{}

func RegisterDbi(name string, constructor DbiConstructor) {
	g_dbregistry[name] = constructor
}

func RegisterDbiEx(name string, constructor DbiExConstructor) {
	g_dbexregistry[name] = constructor
}

func GetDbi(name string, params map[string]interface{}) (dbi gae.DbInt, err error) {
	if fn, has := g_dbregistry[name]; has {
		return fn(params)
	} else {
		err = fmt.Errorf("GetDbi name not found=%s", name)
	}
	return
}

func GetDbiEx(name string, params map[string]interface{}) (dbi gae.DbExInt, err error) {
	if fn, has := g_dbexregistry[name]; has {
		return fn(params)
	} else {
		err = fmt.Errorf("GetDbiEx name not found=%s", name)
	}
	return
}
