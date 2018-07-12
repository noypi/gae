package kvcayley

import (
	"fmt"
)

func SErrApiFailed(api, err string) string {
	return fmt.Sprintf("%s failed, err=%v", api, err)
}
