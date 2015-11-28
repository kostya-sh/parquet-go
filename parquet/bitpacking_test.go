package parquet

import "testing"

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

	// bit width = 4
	{4, []byte{0x00, 0x00, 0x00, 0x00}, [8]int32{0, 0, 0, 0, 0, 0, 0, 0}},
	{4, []byte{0x10, 0x32, 0x54, 0x76}, [8]int32{0, 1, 2, 3, 4, 5, 6, 7}},
	{4, []byte{0x67, 0x45, 0x23, 0x01}, [8]int32{7, 6, 5, 4, 3, 2, 1, 0}},
	{4, []byte{0xEF, 0xCD, 0xAB, 0x89}, [8]int32{15, 14, 13, 12, 11, 10, 9, 8}},
}

func TestUnpack8int32(t *testing.T) {
	for _, test := range unpack8int32Tests {
		unpacker := unpack8Int32FuncForWidth(test.width)
		if got := unpacker(test.data); got != test.values {
			t.Errorf("got %v, want %v", got, test.values)
		}
	}
}
