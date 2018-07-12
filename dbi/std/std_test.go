package dbi

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
