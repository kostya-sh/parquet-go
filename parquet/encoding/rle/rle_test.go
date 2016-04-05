package rle

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestMaxBitWidth(t *testing.T) {
	type test struct {
		bitWidth int
		maxValue int
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
		if v := bitWidth(tt.maxValue); v != tt.bitWidth {
			t.Fatalf("%d case not met %d != %d %d", idx, v, tt.bitWidth) //, bitWidth(tt.maxValue))
		}
	}
}

func repeatInt32(count int, value int32) (a []int32) {
	for i := 0; i < count; i++ {
		a = append(a, value)
	}
	return
}

func packVarInt(value int32, pending ...byte) []byte {
	b := make([]byte, binary.MaxVarintLen32)

	n := binary.PutVarint(b, int64(value))

	return append(b[:n], pending...)
}

func TestRLEDecoder(t *testing.T) {
	var tests = []struct {
		width  uint
		data   []byte
		values []int32
	}{
		// Single RLE run: 1-bit per value, 10 x 0
		{1, packVarInt(10<<1, 0x00), repeatInt32(10, 0)},

		// Single RLE run: 20-bits per value, 300x1
		{20, packVarInt(300<<1, 0x01, 0x00, 0x00), repeatInt32(300, 1)},

		// 2 RLE runs: 1-bit per value, 10x0, 9x1
		{1, append(packVarInt(10<<1, 0x00), packVarInt(9<<1, 0x01)...), append(repeatInt32(10, 0), repeatInt32(9, 1)...)},

		// // 1 bit-packed run: 3 bits per value, 0,1,2,3,4,5,6,7
		// {3, packVarInt((8<<1)|1, 0x88, 0xC6, 0xFA), []int32{0, 1, 2, 3, 4, 5, 6, 7}},

		// // RLE run, bit packed run, RLE run: 2 bits per 8x1, 0, 1, 2, 3, 1, 2, 1, 0, 10x2
		// {
		// 	2,
		// 	[]byte{0x10, 0x01, 0x03, 0xE4, 0x19, 0x14, 0x02},
		// 	[]int32{
		// 		1, 1, 1, 1, 1, 1, 1, 1,
		// 		0, 1, 2, 3, 1, 2, 1, 0,
		// 		2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
		// 	},
		// },
	}

	for i, test := range tests {
		t.Logf("%#v", test.data)
		// var w bytes.Buffer

		// bb := make([]byte, (test.width+7)/8)

		// packLittleEndianInt32(bb, test.value)

		// if _, err := w.Write(b[:n]); err != nil {
		// 	panic(err)
		// }

		values, err := ReadInt32(bytes.NewReader(test.data), test.width, uint(len(test.values)))
		if err != nil {
			t.Errorf("test %d. unexpected error: %s", i, err)
		}
		if !reflect.DeepEqual(values, test.values) {
			t.Errorf("test %d. got %v, want %v", i, values, test.values)
		}
	}
}
