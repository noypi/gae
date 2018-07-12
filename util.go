package gae

import (
	"io/ioutil"
	"net/http"

	"context"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/appengine/remote_api"
)

type ClientType int

const (
	DefaultClient ClientType = iota + 1
	StorageClient
	DatastoreClient
)

func WrapWithRemoteContext(host string, c *http.Client) (ctx context.Context, err error) {
	return remote_api.NewRemoteContext(host, c)
}

func GetTokenSource(ctx context.Context, jwtpath string, scopes ...string) (oauth2.TokenSource, error) {
	bbJwt, err := ioutil.ReadFile(jwtpath)
	if nil != err {
		return nil, err
	}
	conf, err := google.JWTConfigFromJSON(bbJwt, scopes...)
	if err != nil {
		return nil, err
	}

	return conf.TokenSource(ctx), nil
}
