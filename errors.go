package gae

import (
	"fmt"
)

func ErrInvalidParams(s string) error {
	return fmt.Errorf("Error: invalid params, specify '%s'", s)
}

var ErrInternalError = fmt.Errorf("Internal Error.")
var ErrNotSupported = fmt.Errorf("Not Supported.")
