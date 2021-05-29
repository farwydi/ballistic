package file

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

type testStruct struct {
	M int
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

func TestRace(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "ballistic")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, tempFile.Close())
		assert.NoError(t, os.Remove(tempFile.Name()))
	}()

	q, err := NewQueue(tempFile, &testStruct{})
	require.NoError(t, err)

	countWorker := 50
	var c int32
	var wg sync.WaitGroup
	wg.Add(countWorker * 2)
	for i := 0; i < countWorker; i++ {
		go func() {
			defer wg.Done()

			for n := 0; n < 1000; n++ {
				err := q.Push(&testStruct{M: n})
				require.NoError(t, err)
				atomic.AddInt32(&c, 1)
			}
		}()
		go func() {
			defer wg.Done()

			for n := 0; n < 5; n++ {
				m, err := q.Eject(500)
				require.NoError(t, err)
				atomic.AddInt32(&c, -1*int32(len(m)))
			}
		}()
	}
	wg.Wait()

	assert.EqualValues(t, c, q.Len())

	models, err := q.Eject(-1)
	assert.NoError(t, err)
	require.EqualValues(t, c, len(models))
}

func TestPushEjectReopen(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "ballistic")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, tempFile.Close())
		assert.NoError(t, os.Remove(tempFile.Name()))
	}()

	q, err := NewQueue(tempFile, &testStruct{})
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			if q == nil {
				t.FailNow()
				return
			}
			err = q.Push(&testStruct{M: 1})
			assert.NoError(t, err)
			err = q.Push(&testStruct{M: 2})
			assert.NoError(t, err)

			stat, err := tempFile.Stat()
			assert.NoError(t, err)
			assert.NoError(t, tempFile.Close())
			tempFile, err = os.OpenFile(tempFile.Name(), os.O_RDWR, stat.Mode())
			require.NoError(t, err)

			q, err = NewQueue(tempFile, &testStruct{})
			require.NoError(t, err)

			err = q.Push(&testStruct{M: 3})
			assert.NoError(t, err)

			models, err := q.Eject(-1)
			assert.NoError(t, err)

			require.Equal(t, 3, len(models))
			assert.Equal(t, 1, models[0].(*testStruct).M)
			assert.Equal(t, 2, models[1].(*testStruct).M)
			assert.Equal(t, 3, models[2].(*testStruct).M)

			models, err = q.Eject(100)
			assert.NoError(t, err)

			require.Equal(t, 0, len(models))
		})
	}
}
