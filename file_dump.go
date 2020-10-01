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

func NewFileDumper(basePath string,
	failSaveFunc func(query string, args []interface{}, err error),
	failOpenFunc func(err error)) (Dumper, error) {
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		err := os.MkdirAll(basePath, 0644)
		if err != nil {
			return nil, err
		}
	}
	if failSaveFunc == nil {
		failSaveFunc = func(_ string, _ []interface{}, _ error) {
			// Nothing
		}
	}
	if failOpenFunc == nil {
		failOpenFunc = func(_ error) {
			// Nothing
		}
	}
	return &FileDumper{
		basePath:     basePath,
		failSaveFunc: failSaveFunc,
		failOpenFunc: failOpenFunc,
	}, nil
}

type FileDumper struct {
	basePath     string
	failSaveFunc func(query string, args []interface{}, err error)
	failOpenFunc func(err error)
}

func (d *FileDumper) Dump(query string, rows [][]interface{}) {
	for _, row := range rows {
		f, err := ioutil.TempFile(d.basePath, "dump")
		if err != nil {
			d.failSaveFunc(query, row, err)
			return
		}
		ret, err := f.Seek(8, io.SeekStart)
		if err != nil {
			d.failSaveFunc(query, row, err)
			return
		}
		n, err := io.Copy(f, strings.NewReader(query))
		if err != nil {
			d.failSaveFunc(query, row, err)
			return
		}
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			d.failSaveFunc(query, row, err)
			return
		}
		err = binary.Write(f, binary.BigEndian, n)
		if err != nil {
			d.failSaveFunc(query, row, err)
			return
		}
		_, err = f.Seek(ret+n, io.SeekStart)
		if err != nil {
			d.failSaveFunc(query, row, err)
			return
		}
		err = gob.NewEncoder(f).Encode(&row)
		if err != nil {
			d.failSaveFunc(query, row, err)
			return
		}
	}
}

func (d *FileDumper) Return() (exist bool, query string, rows []interface{}) {
	f, err := os.Open(d.basePath)
	if err != nil {
		d.failOpenFunc(err)
		return
	}
	names, err := f.Readdirnames(-1)
	err = f.Close()
	if err != nil {
		d.failOpenFunc(err)
		return
	}
	if len(names) > 0 {
		f, err = os.Open(filepath.Join(d.basePath, names[0]))
		if err != nil {
			d.failOpenFunc(err)
			return
		}
		var n int64 = 0
		err = binary.Read(f, binary.BigEndian, &n)
		if err != nil {
			d.failOpenFunc(err)
			return
		}
		buf := bytes.NewBuffer(nil)
		_, err = io.CopyN(buf, f, n)
		if err != nil {
			d.failOpenFunc(err)
			return
		}
		query = buf.String()
		err = gob.NewDecoder(f).Decode(&rows)
		if err != nil {
			d.failOpenFunc(err)
			return
		}
		err = f.Close()
		if err != nil {
			d.failOpenFunc(err)
			return
		}
		err = os.Remove(f.Name())
		if err != nil {
			d.failOpenFunc(err)
			return
		}
		return true, query, rows
	}
	return false, "", nil
}
