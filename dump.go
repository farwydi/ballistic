package ballistic

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type Dumper interface {
	Dump(query string, rows [][]interface{})
	Return() (exist bool, query string, rows []interface{})
}

type FileDumper struct {
	basePath string
}

func (d *FileDumper) Dump(query string, rows [][]interface{}) {
	for _, row := range rows {
		f, err := ioutil.TempFile(d.basePath, "dump")
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		ret, err := f.Seek(8, io.SeekStart)
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		n, err := io.Copy(f, strings.NewReader(query))
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		err = binary.Write(f, binary.BigEndian, n)
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		_, err = f.Seek(ret+n, io.SeekStart)
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		err = gob.NewEncoder(f).Encode(row)
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
	}
}

func (d *FileDumper) Return() (exist bool, query string, rows []interface{}) {
	f, err := os.Open(d.basePath)
	if err != nil {
		// TODO: Fix conflict
		panic(err)
	}
	names, err := f.Readdirnames(-1)
	err = f.Close()
	if err != nil {
		// TODO: Fix conflict
		panic(err)
	}
	if len(names) > 0 {
		f, err = os.Open(filepath.Join(d.basePath, names[0]))
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		var n int64 = 0
		err = binary.Read(f, binary.BigEndian, &n)
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		buf := bytes.NewBuffer(nil)
		_, err = io.CopyN(buf, f, n)
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		query = buf.String()
		err = gob.NewDecoder(f).Decode(&rows)
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		err = f.Close()
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		err = os.Remove(f.Name())
		if err != nil {
			// TODO: Fix conflict
			panic(err)
		}
		return true, query, rows
	}
	return false, "", nil
}
