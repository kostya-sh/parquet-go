package parquet

import (
	"math"
	"reflect"
	"testing"
)

func rleDecodeAll(w int, data []byte, count int) (a []int32, err error) {
	d := newRLEDecoder(w)
	d.init(data)
	for i := 0; i < count; i++ {
		var v int32
		v, err = d.next()
		if err != nil {
			return a, err
		}
		a = append(a, v)
	}
	return a, nil
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
		values, err := rleDecodeAll(test.width, test.data, len(test.values))
		if err != nil {
			t.Errorf("test %d. unexpected error: %s", i, err)
		}
		if !reflect.DeepEqual(values, test.values) {
			t.Errorf("test %d. got %v, want %v", i, values, test.values)
		}

		// make sure that reading past data returns error
		values, err = rleDecodeAll(test.width, test.data, math.MaxInt32)
		if err == nil {
			t.Errorf("test %d. error expected attempting to read too many values", i)
		} else {
			t.Logf("test %d: %s", i, err)
		}
	}
}

func TestRLEDecoderErrors(t *testing.T) {
	var tests = []struct {
		width int
		data  []byte
		count int
	}{
		// empty data
		{1, []byte{}, 1},

		// Single RLE run: 1-bit per value, 10 x 2: 2 is 2 bit, not 1
		{1, []byte{0x14, 0x02}, 10},

		// Empty bit-packhed run
		// "slice bounds out of range" found with go-fuzz
		{1, []byte{0x09}, 16},
	}

	for i, test := range tests {
		a, err := rleDecodeAll(test.width, test.data, test.count)
		if err == nil {
			t.Errorf("test %d (width %d): error wanted when decoding %v, got %v",
				i, test.width, test.data, a)
		}
	}
}

func TestDecodeRLEValue(t *testing.T) {
	tests := []struct {
		bytes []byte
		value int32
	}{
		{[]byte{0}, 0},
		{[]byte{1}, 1},
		{[]byte{199}, 199},
		{[]byte{0x12, 0x34}, 0x3412},
		{[]byte{0x12, 0x34, 0x56}, 0x563412},
		{[]byte{0x12, 0x34, 0x56, 0x78}, 0x78563412},
		{[]byte{0xFF, 0xFF, 0xFF, 0x7F}, 2147483647},
		{[]byte{0xFF, 0xFF, 0xFF, 0xFF}, -1},
		{[]byte{0x00, 0x00, 0x00, 0x80}, -2147483648},
	}
	for _, test := range tests {
		if got := decodeRLEValue(test.bytes); got != test.value {
			t.Errorf("decodeRLEValue(%v)=%d, want %d", test.bytes, got, test.value)
		}
	}
}
