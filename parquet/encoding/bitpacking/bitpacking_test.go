package bitpacking

import (
	"bytes"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func check(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func bin(values ...string) (out []byte) {

	for _, v := range values {
		v = strings.Replace(v, " ", "", -1)

		b, err := strconv.ParseUint(v, 2, 8)
		if err != nil {
			panic(err)
		}

		out = append(out, byte(b))
	}

	return
}

var testcases = []struct {
	bitWidth uint
	input    []int32
	output   []byte
}{
	// with one bit you can encode 2 values
	{1, []int32{1}, bin("1")},
	{1, []int32{1, 1}, bin("11")},
	{1, []int32{1, 1, 1}, bin("111")},
	{1, []int32{0, 1, 1, 1}, bin("1110")},
	{1, []int32{1, 0, 1, 1, 1}, bin("11101")},

	{1, []int32{1, 1, 1, 1,
		1, 1, 1, 1, 1}, bin("1111 1111", "1")},

	// with two bit you can encode 4 values
	{2, []int32{0, 1, 2, 3}, bin("11 10 01 00",
		"0", // padding
	)},
	{2, []int32{0, 1, 2, 3,
		0, 3, 3, 3}, bin("11 10 01 00", "11 11 11 00")},

	// sample documentation case
	{3, []int32{0, 1, 2, 3, 4, 5, 6, 7},
		bin("10001000", "11000110", "11111010")},

	{8, []int32{0, 1, 2, 4, 8, 16, 32, 64, 128},
		bin("0", "1", "10", "100", "1000",
			"1 0000", "10 0000", "100 0000", "1000 0000",
			"0", "0", "0", "0", "0", "0", "0", // padding
		)},
}

func TestEncoding(t *testing.T) {
	for idx, tc := range testcases {
		var w bytes.Buffer

		enc := NewEncoder(tc.bitWidth, RLE)

		if _, err := enc.Write(&w, tc.input); err != nil {
			t.Fatalf("write: %s", err)
		}

		result := w.Bytes()
		if bytes.Equal(result, tc.output) == false {
			t.Errorf("%d: %#v != %#v", idx, result, tc.output)
		}
	}
}

func TestDecoding(t *testing.T) {
	for idx, tc := range testcases {
		dec := NewDecoder(tc.bitWidth)

		out := make([]int32, len(tc.input))

		if err := dec.Read(bytes.NewReader(tc.output), out); err != nil {
			t.Errorf("%d: %s", idx, err)
		}

		if !reflect.DeepEqual(out, tc.input) {
			t.Logf("%v != %v", out, tc.input)
		}
	}
}

var unpack8int32Tests = []struct {
	width  uint
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

	// bit width = 4
	{4, []byte{0x00, 0x00, 0x00, 0x00}, [8]int32{0, 0, 0, 0, 0, 0, 0, 0}},
	{4, []byte{0x10, 0x32, 0x54, 0x76}, [8]int32{0, 1, 2, 3, 4, 5, 6, 7}},
	{4, []byte{0x67, 0x45, 0x23, 0x01}, [8]int32{7, 6, 5, 4, 3, 2, 1, 0}},
	{4, []byte{0xEF, 0xCD, 0xAB, 0x89}, [8]int32{15, 14, 13, 12, 11, 10, 9, 8}},
}

func TestUnpack8int32(t *testing.T) {
	for _, test := range unpack8int32Tests {
		codec := NewEncoder(test.width, RLE)

		var b bytes.Buffer

		_, err := codec.Write(&b, test.values[:])
		if err != nil {
			t.Errorf("%s", err)
		}

		got := b.Bytes()
		want := test.data
		if !bytes.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		dec := NewDecoder(test.width)
		out := make([]int32, 8)

		if err := dec.Read(&b, out); err != nil {
			t.Errorf("%s", err)
		}

	}
}

func TestBitWidth(t *testing.T) {
	tests := []struct {
		max   uint32
		width uint
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
		if got := GetBitWidthFromMaxInt(test.max); got != test.width {
			t.Errorf("bitWidth(%d)=%d, want %d", test.max, got, test.width)
		}
	}
}

// func TestUnpackLittleEndingInt32(t *testing.T) {
// 	tests := []struct {
// 		bytes []byte
// 		n     int32
// 	}{
// 		{[]byte{0}, 0},
// 		{[]byte{1}, 1},
// 		{[]byte{199}, 199},
// 		{[]byte{0x12, 0x34}, 0x3412},
// 		{[]byte{0x12, 0x34, 0x56}, 0x563412},
// 		{[]byte{0x12, 0x34, 0x56, 0x78}, 0x78563412},
// 	}
// 	for _, test := range tests {
// 		if got := unpackLittleEndianInt32(test.bytes); got != test.n {
// 			t.Errorf("unpackLittleEndianInt32(%v)=%d, want %d", test.bytes, got, test.n)
// 		}
// 	}
// }
