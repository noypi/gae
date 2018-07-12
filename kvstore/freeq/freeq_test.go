package freeq

import (
	"os"
	"testing"

	. "gopkg.in/check.v1"
)

func init() {
	os.Setenv("MY_TEST_ENV", "1")
}

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) {
	TestingT(t)
}

/*
func GetServiceAccountCredentials() *http.Client {
	conf := jwt.Config{
		Email:      "my_email@developer.gserviceaccount.com",
		PrivateKey: []byte("-----BEGIN RSA PRIVATE KEY——\nm_yprivate_key\n-----END RSA PRIVATE KEY-----\n"),
		Scopes:     []string{storage.ScopeFullControl},
		TokenURL:   google.JWTTokenURL,
	}
	return conf.Client(oauth2.NoContext)
}
*/
