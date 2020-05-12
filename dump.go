package ballistic

type Dumper interface {
	Dump(query string, rows [][]interface{})
	Return() (exist bool, query string, rows []interface{})
}
