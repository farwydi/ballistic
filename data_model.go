package ballistic

import "encoding"

type DataModel interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	SQL() string
	ToExec() []interface{}
}
