package parquet

import (
	"math"
	"testing"
)

func TestUnpack8int32(t *testing.T) {
	for _, test := range unpack8int32Tests {
		unpacker := unpack8Int32FuncByWidth[test.width]
		if got := unpacker(test.data); got != test.values {
			t.Errorf("unpack for width %d: got %v, want %v", test.width, got, test.values)
		}
	}
}

func TestBitWidth(t *testing.T) {
	tests := []struct {
		max   int
		width int
	}{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
		{6, 3},
		{7, 3},
		{8, 4},
		{9, 4},
		{257, 9},
		{math.MaxInt32, 31},
		{math.MaxUint32, 32}, // TODO: will this work on 32-bit system?
	}
	for _, test := range tests {
		if got := bitWidth(test.max); got != test.width {
			t.Errorf("bitWidth(%d)=%d, want %d", test.max, got, test.width)
		}
	}
}

func TestUnpackLittleEndingInt32(t *testing.T) {
	tests := []struct {
		bytes []byte
		n     int32
	}{
		{[]byte{0}, 0},
		{[]byte{1}, 1},
		{[]byte{199}, 199},
		{[]byte{0x12, 0x34}, 0x3412},
		{[]byte{0x12, 0x34, 0x56}, 0x563412},
		{[]byte{0x12, 0x34, 0x56, 0x78}, 0x78563412},
	}
	for _, test := range tests {
		if got := unpackLittleEndianInt32(test.bytes); got != test.n {
			t.Errorf("unpackLittleEndianInt32(%v)=%d, want %d", test.bytes, got, test.n)
		}
	}
}
