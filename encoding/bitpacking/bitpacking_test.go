package bitpacking

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func check(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func binary(values ...string) (out []byte) {
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

func TestEncodingRange1Bit(t *testing.T) {
	testcases := []struct {
		bitWidth int
		input    []int64
		output   []byte
	}{
		// with one bit you can encode 2 values
		{1, []int64{1}, binary("1")},
		{1, []int64{1, 1}, binary("11")},
		{1, []int64{1, 1, 1}, binary("111")},
		{1, []int64{0, 1, 1, 1}, binary("1110")},
		{1, []int64{1, 0, 1, 1, 1}, binary("11101")},

		{1, []int64{1, 1, 1, 1,
			1, 1, 1, 1, 1}, binary("1111 1111", "1")},

		// with two bit you can encode 4 values
		{2, []int64{0, 1, 2, 3}, binary("11 10 01 00")},
		{2, []int64{0, 1, 2, 3,
			0, 3, 3, 3}, binary("11 10 01 00", "11 11 11 00")},

		// sample documentation case
		{3, []int64{0, 1, 2, 3, 4, 5, 6, 7},
			binary("10001000", "11000110", "11111010")},

		{8, []int64{0, 1, 2, 4, 8, 16, 32, 64, 128},
			binary("0", "1", "10", "100", "1000",
				"1 0000", "10 0000", "100 0000", "1000 0000")},
	}

	for idx, tc := range testcases {
		var w bytes.Buffer
		enc := NewEncoder(&w, tc.bitWidth)
		for _, value := range tc.input {
			if err := enc.Write(value); err != nil {
				t.Fatal(err)
			}
		}

		if err := enc.Flush(); err != nil {
			t.Fatal(err)
		}

		if bytes.Equal(w.Bytes(), tc.output) == false {
			t.Fatalf("%d: %#v != %#v", idx, w.Bytes(), tc.output)
		}
	}
}
