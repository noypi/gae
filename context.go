package gae

import (
	"context"
)

type _gaecontextKeyName int

const (
	GAEContextKey _gaecontextKeyName = iota
)

type Store interface {
	Get(k interface{}) (v interface{}, exists bool)
	Set(k, v interface{})
}

func GetGAEContext(ctx context.Context) context.Context {
	c := ctx.(Store)
	if o, exists := c.Get(GAEContextKey); exists {
		return o.(context.Context)
	}

	return nil
}

func PutGAEContext(ctx, gaectx context.Context) {
	c := ctx.(Store)
	c.Set(GAEContextKey, gaectx)
}
