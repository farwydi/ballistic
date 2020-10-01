package ballistic

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFileDumper_DirShouldBeMake(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "_test")
	require.NoError(t, err)

	tmpDir = filepath.Join(tmpDir, "test", "test")
	_, err = NewFileDumper(tmpDir, nil, nil)
	assert.NoError(t, err)

	assert.DirExists(t, tmpDir)
	os.RemoveAll(tmpDir)
}

func TestFileDumper_Dump(t *testing.T) {
	type fields struct {
		basePath string
	}
	type args struct {
		query string
		rows  [][]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "",
			fields: fields{
				basePath: "./dump_test",
			},
			args: args{
				query: "INSERT INTO",
				rows: [][]interface{}{
					{
						"t1",
						1,
						false,
						int16(2),
						uint64(3),
					},
					{
						"t1",
						1,
						false,
						int16(2),
						uint64(3),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &FileDumper{
				basePath: tt.fields.basePath,
			}
			d.Dump(tt.args.query, tt.args.rows)
		})
	}
}

func TestFileDumper_Return(t *testing.T) {
	type fields struct {
		basePath string
	}
	tests := []struct {
		name      string
		fields    fields
		wantExist bool
		wantQuery string
		wantRows  []interface{}
	}{
		{
			name: "",
			fields: fields{
				basePath: "./dump_test",
			},
			wantExist: true,
			wantQuery: "INSERT INTO",
			wantRows: []interface{}{
				"t1",
				1,
				false,
				int16(2),
				uint64(3),
			},
		},
		{
			name: "",
			fields: fields{
				basePath: "./dump_test",
			},
			wantExist: true,
			wantQuery: "INSERT INTO",
			wantRows: []interface{}{
				"t1",
				1,
				false,
				int16(2),
				uint64(3),
			},
		},
		{
			name: "",
			fields: fields{
				basePath: "./dump_test",
			},
			wantExist: false,
			wantQuery: "",
			wantRows:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &FileDumper{
				basePath: tt.fields.basePath,
			}
			gotExist, gotQuery, gotRows := d.Return()
			if gotExist != tt.wantExist {
				t.Errorf("Return() gotExist = %v, want %v", gotExist, tt.wantExist)
			}
			if gotQuery != tt.wantQuery {
				t.Errorf("Return() gotQuery = %v, want %v", gotQuery, tt.wantQuery)
			}
			if !reflect.DeepEqual(gotRows, tt.wantRows) {
				t.Errorf("Return() gotRows = %v, want %v", gotRows, tt.wantRows)
			}
		})
	}
}
