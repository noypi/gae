package tools

import (
	"github.com/noypi/gae"
	dumdbi "github.com/noypi/gae/dbi/dummy"
	deflog "github.com/noypi/gae/logi/default"
	"github.com/noypi/gae/registry"
	defweb "github.com/noypi/gae/webi/default"
)

func NewDefaultTk() gae.Tk {
	o := new(GaeTools)
	o.Logi = deflog.LogIntDefault{}
	o.Webi = defweb.NewWebiDefault()
	var err error
	o.Dbi, err = registry.GetDbi(dumdbi.Name, nil)
	if nil != err {
		o.Logi.Criticalf("NewDefaultTk() GetDbi err=%v", err)
	}

	return o
}
