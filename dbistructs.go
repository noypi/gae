package gae

import (
	"context"

	cloudds "cloud.google.com/go/datastore"
	localds "google.golang.org/appengine/datastore"
)

func (o DbiKeyArr) AppendArr(a DbiKeyArr) (out DbiKeyArr) {
	if 0 < len(o.Cloud) || 0 < len(a.Cloud) {
		if nil == o.Cloud {
			o.Cloud = []*cloudds.Key{}
		}
		out.Cloud = append(o.Cloud, a.Cloud...)
	}

	if 0 < len(o.Local) || 0 < len(a.Local) {
		if nil == o.Local {
			o.Local = []*localds.Key{}
		}
		out.Local = append(o.Local, a.Local...)
	}

	return
}

func (o DbiKeyArr) Append(a DbiKey) (out DbiKeyArr) {
	if nil != a.Cloud {
		if nil == o.Cloud {
			o.Cloud = []*cloudds.Key{}
		}
		out.Cloud = append(o.Cloud, a.Cloud)
	}

	if nil != a.Local {
		if nil == o.Local {
			o.Local = []*localds.Key{}
		}
		out.Local = append(o.Local, a.Local)
	}

	return
}

func (o DbiKeyArr) ToStringArray() (ks []string) {
	if 0 < len(o.Cloud) {
		ks = make([]string, len(o.Cloud))
		for i, k := range o.Cloud {
			ks[i] = k.Name
		}
	} else if 0 < len(o.Local) {
		ks = make([]string, len(o.Local))
		for i, k := range o.Local {
			ks[i] = k.StringID()
		}
	}

	return
}

func (o DbiKeyArr) ToDbiKeyArray() (ks []DbiKey) {
	if 0 < len(o.Cloud) {
		ks = make([]DbiKey, len(o.Cloud))
		for i, k := range o.Cloud {
			ks[i].Cloud = k
		}
	} else if 0 < len(o.Local) {
		ks = make([]DbiKey, len(o.Local))
		for i, k := range o.Local {
			ks[i].Local = k
		}
	}

	return
}

func (o DbiKeyArr) Len() (n int) {
	if nil != o.Cloud {
		n = len(o.Cloud)
	} else if nil != o.Local {
		n = len(o.Local)
	}
	return
}

func (o DbiKeyArr) ForEach(fn func(DbiKey) (bContinue bool)) {
	if nil != o.Cloud {
		for _, k := range o.Cloud {
			if !fn(DbiKey{Cloud: k}) {
				break
			}
		}
	} else if nil != o.Local {
		for _, k := range o.Local {
			if !fn(DbiKey{Local: k}) {
				break
			}
		}
	}
	return
}

func (o DbiKey) StringID() (s string) {
	if nil != o.Cloud {
		s = o.Cloud.Name
	} else if nil != o.Local {
		s = o.Local.StringID()
	}
	return
}

func (o DbiKey) IntID() (n int64) {
	if nil != o.Cloud {
		n = o.Cloud.ID
	} else if nil != o.Local {
		n = o.Local.IntID()
	}
	return
}

func (o DbiKey) IsNil() bool {
	return nil == o.Local && nil == o.Cloud
}

func (o *DbiIterator) Next(dst interface{}) (DbiKey, error) {
	if nil != o.Cloud {
		k, err := o.Cloud.Next(dst)
		return DbiKey{Cloud: k}, err
	} else if nil != o.Local {
		k, err := o.Local.Next(dst)
		return DbiKey{Local: k}, err
	}

	return DbiKey{}, ErrInternalError

}

func (o DbiIterator) IsNil() bool {
	return nil == o.Cloud && nil == o.Local
}

func (o DbiQuery) Count(c context.Context) (n int, err error) {
	if nil != o.Cloud {
		return 0, ErrNotSupported
	} else if nil != o.Local {
		return o.Local.Count(c)
	}

	return
}

func (o DbiQuery) Order(fieldName string) (q DbiQuery) {
	if nil != o.Cloud {
		q.Cloud = o.Cloud.Order(fieldName)
	} else if nil != o.Local {
		q.Local = o.Local.Order(fieldName)
	}

	q.C = o.C

	return
}

func (o DbiQuery) Filter(filterStr string, value interface{}) (q DbiQuery) {
	if nil != o.Cloud {
		q.Cloud = o.Cloud.Filter(filterStr, value)
	} else if nil != o.Local {
		q.Local = o.Local.Filter(filterStr, value)
	}

	q.C = o.C

	return
}

func (o DbiQuery) KeysOnly() (q DbiQuery) {
	if nil != o.Cloud {
		q.Cloud = o.Cloud.KeysOnly()
	} else if nil != o.Local {
		q.Local = o.Local.KeysOnly()
	}

	q.C = o.C

	return
}
