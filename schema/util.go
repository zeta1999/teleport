package schema

import (
	"strings"
)

func squish(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
