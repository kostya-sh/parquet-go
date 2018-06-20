package parquet

import (
	"math"
	"testing"
)

var unpack8int32Tests = []struct {
	width  int
	data   []byte
	values [8]int32
}{
	// bit width = 1
	{1, []byte{0x00}, [8]int32{0, 0, 0, 0, 0, 0, 0, 0}},
	{1, []byte{0xFF}, [8]int32{1, 1, 1, 1, 1, 1, 1, 1}},
	{1, []byte{0x4D}, [8]int32{1, 0, 1, 1, 0, 0, 1, 0}},

	// bit width = 2
	{2, []byte{0x55, 0x55}, [8]int32{1, 1, 1, 1, 1, 1, 1, 1}},
	{2, []byte{0xAA, 0xAA}, [8]int32{2, 2, 2, 2, 2, 2, 2, 2}},
	{2, []byte{0xA4, 0x41}, [8]int32{0, 1, 2, 2, 1, 0, 0, 1}},

	// bit width = 3
	{3, []byte{0x00, 0x00, 0x00}, [8]int32{0, 0, 0, 0, 0, 0, 0, 0}},
	{3, []byte{0x88, 0xC6, 0xFA}, [8]int32{0, 1, 2, 3, 4, 5, 6, 7}},
	{3, []byte{0x77, 0x39, 0x05}, [8]int32{7, 6, 5, 4, 3, 2, 1, 0}},
	{3, []byte{0x23, 0xA2, 0x11}, [8]int32{3, 4, 0, 1, 2, 3, 4, 0}},

	// bit width = 4
	{4, []byte{0x00, 0x00, 0x00, 0x00}, [8]int32{0, 0, 0, 0, 0, 0, 0, 0}},
	{4, []byte{0x10, 0x32, 0x54, 0x76}, [8]int32{0, 1, 2, 3, 4, 5, 6, 7}},
	{4, []byte{0x67, 0x45, 0x23, 0x01}, [8]int32{7, 6, 5, 4, 3, 2, 1, 0}},
	{4, []byte{0xEF, 0xCD, 0xAB, 0x89}, [8]int32{15, 14, 13, 12, 11, 10, 9, 8}},

	// bit width = 5
	{5, []byte{0x00, 0x00, 0x00, 0x00, 0x00}, [8]int32{0, 0, 0, 0, 0, 0, 0, 0}},
	{5, []byte{0x20, 0x88, 0x41, 0x8A, 0x39}, [8]int32{0, 1, 2, 3, 4, 5, 6, 7}},
	{5, []byte{0xC7, 0x14, 0x32, 0x44, 0x00}, [8]int32{7, 6, 5, 4, 3, 2, 1, 0}},
	{5, []byte{0xDF, 0x77, 0xBE, 0x75, 0xC6}, [8]int32{31, 30, 29, 28, 27, 26, 25, 24}},

	// bit width = 8
	{8, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, [8]int32{1, 2, 3, 4, 5, 6, 7, 8}},

	// bit width = 13
	{13, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, [8]int32{0, 0, 0, 0, 0, 0, 0, 0}},
	{13, []byte{0x00, 0x20, 0x00, 0x08, 0x80, 0x01, 0x40, 0x00, 0x0A, 0x80, 0x01, 0x38, 0x00}, [8]int32{0, 1, 2, 3, 4, 5, 6, 7}},
	{13, []byte{0x07, 0xC0, 0x00, 0x14, 0x00, 0x02, 0x30, 0x00, 0x04, 0x40, 0x00, 0x00, 0x00}, [8]int32{7, 6, 5, 4, 3, 2, 1, 0}},
	{13, []byte{0xFF, 0xDF, 0xFF, 0xF7, 0x7F, 0xFE, 0xBF, 0xFF, 0xF5, 0x7F, 0xFE, 0xC7, 0xFF}, [8]int32{8191, 8190, 8189, 8188, 8187, 8186, 8185, 8184}},
}

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
