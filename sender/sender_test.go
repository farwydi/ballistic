package sender

import (
	"testing"
)

func TestAppendMap(t *testing.T) {
	m := map[string][][]interface{}{}

	m["test"] = append(m["test"], nil)
	m["test"] = append(m["test"], nil)
}
