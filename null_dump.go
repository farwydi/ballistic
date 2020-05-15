package ballistic

func NewNullDumper() Dumper {
	return &NullDumper{}
}

type NullDumper struct {
}

func (d *NullDumper) Dump(string, [][]interface{}) {
	return
}

func (d *NullDumper) Return() (exist bool, query string, rows []interface{}) {
	return false, "", nil
}
