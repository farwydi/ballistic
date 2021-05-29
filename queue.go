package ballistic

import "encoding"

type Queue interface {
	Push(model encoding.BinaryMarshaler) error
	Eject(limit int) (models []interface{}, err error)
	Len() int
}
