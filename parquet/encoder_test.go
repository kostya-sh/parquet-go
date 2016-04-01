package parquet

import (
	"bytes"
	"math/rand"
	"testing"
)

var schema = `
{
  "type" : "record",
  "name" : "Weather",
  "namespace" : "test",
  "doc" : "A weather reading.",
  "fields" : [ {
    "name" : "station",
    "type" : "string"
  }, {
    "name" : "time",
    "type" : "long"
  }, {
    "name" : "temp",
    "type" : "int"
  } ]
}
`

func genint32(n int) []int32 {
	values := make([]int32, n)
	for i := 0; i < n; i++ {
		values[i] = rand.Int31()
	}
	return values
}

func genint64(n int) []int64 {
	values := make([]int64, n)
	for i := 0; i < n; i++ {
		values[i] = rand.Int63()
	}
	return values
}

func genfloat32(n int) []float32 {
	values := make([]float32, n)
	for i := 0; i < n; i++ {
		values[i] = rand.Float32()
	}
	return values
}

func genfloat64(n int) []float64 {
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		values[i] = rand.Float64()
	}
	return values
}

func genbyte(maxsize int, n int) [][]byte {
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := rand.Intn(maxsize)
		values[i] = make([]byte, k)
		for j := 0; j < k; j++ {
			values[i][j] = byte(rand.Int())
		}
	}
	return values
}

func TestCodec(t *testing.T) {
	schema := NewSchema()
	var buff bytes.Buffer
	schema.AddColumnFromSpec("station: string REQUIRED")
	schema.AddColumnFromSpec("timestamp: long REQUIRED")
	schema.AddColumnFromSpec("temperature: int REQUIRED")

	enc := NewEncoder(schema, &buff)

	raw := []map[string]interface{}{
		{"station": "011990-99999", "time": int64(-619524000000), "temp": int32(0)},
		{"station": "011990-99999", "time": int64(-619506000000), "temp": int32(22)},
		{"station": "011990-99999", "time": int64(-619484400000), "temp": int32(-11)},
		{"station": "012650-99999", "time": int64(-655531200000), "temp": int32(111)},
		{"station": "012650-99999", "time": int64(-655509600000), "temp": int32(78)},
	}

	err := enc.WriteRecords(raw)
	if err != nil {
		t.Fatal(err)
	}

	err := enc.Close()
	if err != nil {
		t.Fatal(err)
	}

}
