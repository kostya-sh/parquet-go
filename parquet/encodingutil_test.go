package parquet

import (
	"testing"
)

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
