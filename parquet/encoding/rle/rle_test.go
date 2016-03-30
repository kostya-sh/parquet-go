package rle

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func TestMaxBitWidth(t *testing.T) {
	type test struct {
		bitWidth uint
		maxValue uint32
	}

	tc := []test{
		{0, 0},
		{1, 1},
		{2, 2},
		{2, 3},
		{3, 4},
		{3, 5},
		{3, 6},
		{3, 7},
		{4, 8},
		{4, 15},
		{5, 16},
		{5, 31},
		{6, 32},
		{6, 63},
		{7, 64},
		{7, 127},
		{8, 128},
		{8, 255},
	}

	for idx, tt := range tc {
		if v := uint(math.Log2(float64(tt.maxValue + 1))); v != tt.bitWidth {
			t.Fatalf("%d case not met %d != %d", idx, v, tt.bitWidth)
		}
	}
}

func repeatInt32(count int, value int64) (a []int64) {
	for i := 0; i < count; i++ {
		a = append(a, value)
	}
	return
}

func TestRLEDecoder(t *testing.T) {
	var tests = []struct {
		width  uint
		data   []byte
		values []int64
	}{
		// Single RLE run: 1-bit per value, 10 x 0
		{1, []byte{0x14, 0x00}, repeatInt32(10, 0)},

		// Single RLE run: 20-bits per value, 300x1
		{20, []byte{0xD8, 0x04, 0x01, 0x00, 0x00}, repeatInt32(300, 1)},

		// 2 RLE runs: 1-bit per value, 10x0, 9x1
		{1, []byte{0x14, 0x00, 0x12, 0x01}, append(repeatInt32(10, 0), repeatInt32(9, 1)...)},

		// 1 bit-packed run: 3 bits per value, 0,1,2,3,4,5,6,7
		{3, []byte{0x03, 0x88, 0xC6, 0xFA}, []int64{0, 1, 2, 3, 4, 5, 6, 7}},

		// RLE run, bit packed run, RLE run: 2 bits per 8x1, 0, 1, 2, 3, 1, 2, 1, 0, 10x2
		{
			2,
			[]byte{0x10, 0x01, 0x03, 0xE4, 0x19, 0x14, 0x02},
			[]int64{
				1, 1, 1, 1, 1, 1, 1, 1,
				0, 1, 2, 3, 1, 2, 1, 0,
				2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			},
		},
	}

	for i, test := range tests {
		values, err := ReadInt64(bytes.NewReader(test.data), test.width, uint(len(test.values)))
		if err != nil {
			t.Errorf("test %d. unexpected error: %s", i, err)
		}
		if !reflect.DeepEqual(values, test.values) {
			t.Errorf("test %d. got %v, want %v", i, values, test.values)
		}

	}
}
