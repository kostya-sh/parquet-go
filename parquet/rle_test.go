package parquet

import (
	"math"
	"reflect"
	"testing"
)

func rle32DecodeAll(w int, data []byte, count int) (a []int32, err error) {
	d := newRLE32Decoder(w)
	d.init(data)
	for i := 0; i < count; i++ {
		var next int32
		next, err = d.next()
		if err != nil {
			return
		}
		a = append(a, next)
	}
	return
}

func repeatInt32(count int, value int32) (a []int32) {
	for i := 0; i < count; i++ {
		a = append(a, value)
	}
	return
}

func TestRLEDecoder(t *testing.T) {
	var tests = []struct {
		width  int
		data   []byte
		values []int32
	}{
		// Single RLE run: 1-bit per value, 10 x 0
		{1, []byte{0x14, 0x00}, repeatInt32(10, 0)},

		// Single RLE run: 20-bits per value, 300x1
		{20, []byte{0xD8, 0x04, 0x01, 0x00, 0x00}, repeatInt32(300, 1)},

		// 2 RLE runs: 1-bit per value, 10x0, 9x1
		{1, []byte{0x14, 0x00, 0x12, 0x01}, append(repeatInt32(10, 0), repeatInt32(9, 1)...)},

		// 1 bit-packed run: 3 bits per value, 0,1,2,3,4,5,6,7
		{3, []byte{0x03, 0x88, 0xC6, 0xFA}, []int32{0, 1, 2, 3, 4, 5, 6, 7}},

		// RLE run, bit packed run, RLE run: 2 bits per 8x1, 0, 1, 2, 3, 1, 2, 1, 0, 10x2
		{
			2,
			[]byte{0x10, 0x01, 0x03, 0xE4, 0x19, 0x14, 0x02},
			[]int32{
				1, 1, 1, 1, 1, 1, 1, 1,
				0, 1, 2, 3, 1, 2, 1, 0,
				2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			},
		},

		// 1 bit packed run
		{
			3,
			[]byte{7, 136, 70, 68, 35, 162, 17, 209, 136, 104},
			[]int32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4},
		},

		// unpadded bit-packed encoding
		// from github.com/Parquet/parquet-compatibility/parquet-testdata/impala/1.1.1-NONE/nation.impala.parquet
		{
			5,
			[]byte{9, 32, 136, 65, 138, 57, 40, 169, 197, 154, 123, 48, 202, 73, 171, 189, 24},
			[]int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24},
		},
	}

	for i, test := range tests {
		values, err := rle32DecodeAll(test.width, test.data, len(test.values))
		if err != nil {
			t.Errorf("test %d. unexpected error: %s", i, err)
		}
		if !reflect.DeepEqual(values, test.values) {
			t.Errorf("test %d. got %v, want %v", i, values, test.values)
		}

		// make sure that reading past data returns error
		values, err = rle32DecodeAll(test.width, test.data, math.MaxInt32)
		if err == nil {
			t.Errorf("test %d. error expected attempting to read too many values", i)
		} else {
			t.Logf("test %d: %s", i, err)
		}
	}
}

// TODO: tests for bogus data
