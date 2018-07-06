package parquet

import (
	"math"
	"testing"
)

func TestBitWidth16(t *testing.T) {
	tests := []struct {
		max   uint16
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
		{math.MaxInt16, 15},
		{math.MaxUint16, 16},
	}
	for _, test := range tests {
		if got := bitWidth16(test.max); got != test.width {
			t.Errorf("bitWidth16(%d)=%d, want %d", test.max, got, test.width)
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

func TestZigZagVarInt32(t *testing.T) {
	tests := []struct {
		bytes []byte
		value int32
	}{
		{[]byte{0x00}, 0},
		{[]byte{0x01}, -1},
		{[]byte{0x2}, 1},
		{[]byte{0x3}, -2},
		{[]byte{0x04}, 2},
		{[]byte{0xFE, 0xFF, 0xFF, 0xFF, 0x0F}, 2147483647},
		{[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}, -2147483648},
		{[]byte{0x80, 0x89, 0x0F}, 123456},
		{[]byte{0xE1, 0xEF, 0x4F}, -654321},
	}
	for _, test := range tests {
		if got, n := zigZagVarInt32(test.bytes); got != test.value || n != len(test.bytes) {
			t.Errorf("zigZagVarInt32(%v)=%d, %d, want %d, %d",
				test.bytes, got, n, test.value, len(test.bytes))
		}
	}
}

func TestZigZagVarInt64(t *testing.T) {
	tests := []struct {
		bytes []byte
		value int64
	}{
		{[]byte{0x00}, 0},
		{[]byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}, 9223372036854775807},
		{[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}, -9223372036854775808},
		{[]byte{0x80, 0x89, 0x0F}, 123456},
		{[]byte{0xE1, 0xEF, 0x4F}, -654321},
	}
	for _, test := range tests {
		if got, n := zigZagVarInt64(test.bytes); got != test.value || n != len(test.bytes) {
			t.Errorf("zigZagVarInt64(%v)=%d, %d, want %d, %d",
				test.bytes, got, n, test.value, len(test.bytes))
		}
	}
}
