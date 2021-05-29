// +build integration

package ballistic_test

import (
	"context"
	"database/sql"
	"encoding/json"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/farwydi/ballistic/sender"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var b2i = map[bool]int8{
	false: 0,
	true:  1,
}

// testStruct1
type testStruct1 struct {
	RecordTime time.Time `json:"record_time"`
	StringVal  string    `json:"string_val"`
	BoolVar    bool      `json:"bool_var"`
	Int32Val   int32     `json:"int_32_val"`
	UInt64Val  uint64    `json:"u_int_64_val"`
	IntVal     int       `json:"int_val"`
	Float32Val float64   `json:"float_32_val"`
	Float64Val float64   `json:"float_64_val"`
}

func (t testStruct1) MarshalBinary() (data []byte, err error) {
	return json.Marshal(t)
}

func (t *testStruct1) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

func (t *testStruct1) SQL() string {
	return "INSERT INTO test.table_1" +
		"(record_time, string_val, bool_var, int_32_val, u_int_64_val, int_val, float_32_val, float_64_val)" +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
}

func (t *testStruct1) ToExec() []interface{} {
	return []interface{}{
		t.RecordTime,
		t.StringVal,
		b2i[t.BoolVar],
		t.Int32Val,
		t.UInt64Val,
		t.IntVal,
		t.Float32Val,
		t.Float64Val,
	}
}

// testStruct2
type testStruct2 struct {
	RecordTime time.Time `json:"record_time"`
	StringVal  string    `json:"string_val"`
	BoolVar    bool      `json:"bool_var"`
	Int32Val   int32     `json:"int_32_val"`
	UInt64Val  uint64    `json:"u_int_64_val"`
	IntVal     int       `json:"int_val"`
	Float32Val float64   `json:"float_32_val"`
	Float64Val float64   `json:"float_64_val"`
}

func (t testStruct2) MarshalBinary() (data []byte, err error) {
	return json.Marshal(t)
}

func (t *testStruct2) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

func (t *testStruct2) SQL() string {
	return "INSERT INTO test.table_2" +
		"(record_time, string_val, bool_var, int_32_val, u_int_64_val, int_val, float_32_val, float_64_val)" +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
}

func (t *testStruct2) ToExec() []interface{} {
	return []interface{}{
		t.RecordTime,
		t.StringVal,
		b2i[t.BoolVar],
		t.Int32Val,
		t.UInt64Val,
		t.IntVal,
		t.Float32Val,
		t.Float64Val,
	}
}

func TestSenderInRealDatabase(t *testing.T) {
	time.Local = time.UTC

	tempDir, err := ioutil.TempDir("", "ballistic")
	require.NoError(t, err)
	defer func() {
		// TODO: remove temp dir
	}()

	conn, err := sql.Open("clickhouse",
		"tcp://localhost:9000?&database=test&read_timeout=10&write_timeout=20")
	require.NoError(t, err)

	s := sender.NewSender(conn, sender.Config{
		UseMemoryFallback: true,
		FileWorkspace:     tempDir,
	})

	s.RunPusher(100*time.Millisecond, 1000)

	var it int32
	var wg sync.WaitGroup
	for n := 0; n < 10; n++ {
		wg.Add(1)
		go func() {
			var err error
			defer wg.Done()

			for i := 0; i < 2000; i++ {
				err = s.Push(&testStruct1{
					RecordTime: time.Now(),
					StringVal:  "test",
					BoolVar:    true,
					Int32Val:   rand.Int31(),
					UInt64Val:  rand.Uint64(),
					IntVal:     rand.Int(),
					Float32Val: rand.Float64(),
					Float64Val: rand.Float64(),
				})
				assert.NoError(t, err)
				err = s.Push(&testStruct2{
					RecordTime: time.Now(),
					StringVal:  "test",
					BoolVar:    true,
					Int32Val:   rand.Int31(),
					UInt64Val:  rand.Uint64(),
					IntVal:     rand.Int(),
					Float32Val: rand.Float64(),
					Float64Val: rand.Float64(),
				})
				assert.NoError(t, err)
				atomic.AddInt32(&it, 1)

				if atomic.LoadInt32(&it)%500 == 0 {
					time.Sleep(50 * time.Millisecond)
					t.Logf("insert: %d", atomic.LoadInt32(&it))
				}
			}
		}()
	}

	wg.Wait()
	s.Stop(true)

	// test data
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var count int
	err = conn.QueryRowContext(ctx, `SELECT count(1) FROM test.table_1`).Scan(&count)
	assert.NoError(t, err)
	assert.EqualValues(t, count, it)

	err = conn.QueryRowContext(ctx, `SELECT count(1) FROM test.table_2`).Scan(&count)
	assert.NoError(t, err)
	assert.EqualValues(t, count, it)
}
