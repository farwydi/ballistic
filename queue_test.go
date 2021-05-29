package ballistic_test

import (
	"encoding/json"
	"fmt"
	"github.com/farwydi/ballistic"
	"github.com/farwydi/ballistic/queue/file"
	"github.com/farwydi/ballistic/queue/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

type testStruct struct {
	T  time.Time
	S  string
	Bs []byte
}

func (t *testStruct) SQL() string {
	return "test"
}

func (t *testStruct) ToExec() []interface{} {
	return nil
}

func (t *testStruct) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

func (t testStruct) MarshalBinary() (data []byte, err error) {
	return json.Marshal(t)
}

func TestQueueLimit(t *testing.T) {
	var tempFiles []*os.File

	defer func() {
		for _, tempFile := range tempFiles {
			assert.NoError(t, tempFile.Close())
			assert.NoError(t, os.Remove(tempFile.Name()))
		}
	}()

	testsType := []struct {
		name string
		Type func() ballistic.Queue
	}{
		{
			name: "Memory",
			Type: func() ballistic.Queue {
				return memory.NewQueue()
			},
		},
		{
			name: "File",
			Type: func() ballistic.Queue {
				tempFile, err := ioutil.TempFile("", "test")
				require.NoError(t, err)
				tempFiles = append(tempFiles, tempFile)
				q, err := file.NewQueue(tempFile, &testStruct{})
				require.NoError(t, err)
				return q
			},
		},
	}
	for _, testType := range testsType {
		t.Run(testType.name, func(t *testing.T) {
			testsLimit := []struct {
				limit int
			}{
				{limit: 0},
				{limit: 1},
				{limit: 2},
				{limit: 3},
			}
			for _, testLimit := range testsLimit {
				t.Run(fmt.Sprintf("Limit=%d", testLimit.limit), func(t *testing.T) {
					q := testType.Type()
					err := q.Push(&testStruct{
						T:  time.Date(2021, 04, 29, 20, 1, 34, 561, time.UTC),
						S:  "string",
						Bs: []byte("test data"),
					})
					assert.NoError(t, err)
					err = q.Push(&testStruct{
						T:  time.Date(2021, 04, 29, 20, 5, 34, 561, time.UTC),
						S:  "string x",
						Bs: []byte("test data x"),
					})
					assert.NoError(t, err)
					models, err := q.Eject(testLimit.limit)
					assert.NoError(t, err)
					assert.LessOrEqual(t, len(models), testLimit.limit)

					if testLimit.limit > 0 {
						require.NotZero(t, len(models))

						d1, ok := models[0].(*testStruct)
						assert.True(t, ok)
						require.NotNil(t, d1)
						assert.Equal(t, d1.T, time.Date(2021, 04, 29, 20, 1, 34, 561, time.UTC))
						assert.Equal(t, d1.S, "string")
						assert.Equal(t, d1.Bs, []byte("test data"))
					}
				})
			}
		})
	}
}

func TestBaseQueue(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "test")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, tempFile.Close())
		assert.NoError(t, os.Remove(tempFile.Name()))
	}()
	fileQueue, err := file.NewQueue(tempFile, &testStruct{})
	require.NoError(t, err)

	testsType := []struct {
		name string
		Type ballistic.Queue
	}{
		{
			name: "Memory",
			Type: memory.NewQueue(),
		},
		{
			name: "File",
			Type: fileQueue,
		},
	}
	for _, testType := range testsType {
		t.Run(testType.name, func(t *testing.T) {
			q := testType.Type

			err = q.Push(&testStruct{S: "1"})
			assert.NoError(t, err)
			err = q.Push(&testStruct{S: "2"})
			assert.NoError(t, err)

			_, err = q.Eject(100)
			assert.NoError(t, err)

			err = q.Push(&testStruct{S: "3"})
			assert.NoError(t, err)
			err = q.Push(&testStruct{S: "4"})
			assert.NoError(t, err)

			models, err := q.Eject(100)
			assert.NoError(t, err)

			require.Equal(t, 2, len(models))
			assert.Equal(t, "3", models[0].(*testStruct).S)
			assert.Equal(t, "4", models[1].(*testStruct).S)
		})
	}
}
