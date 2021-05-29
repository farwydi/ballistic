package file

import "sync"

var bsPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 4)
	},
}
