package parquet

import (
	"testing"
)

func TestBooleanPlainDecoder(t *testing.T) {
	testValuesDecoder(t, &booleanPlainDecoder{}, []decoderTestCase{
		{
			data:    []byte{0x00},
			decoded: []interface{}{false, false, false, false, false},
		},
		{
			data:    []byte{0xFF},
			decoded: []interface{}{true, true, true},
		},
		{
			data:    []byte{0x06E}, // 0b01101110
			decoded: []interface{}{false, true, true, true, false, true, true, false},
		},
		{
			data:    []byte{0xFF, 0x06E}, // 0b11111111 0b01101110
			decoded: []interface{}{true, true, true, true, true, true, true, true, false, true, true, true, false, true, true},
		},
	})
}

func TestBooleanRLEDecoder(t *testing.T) {
	testValuesDecoder(t, &booleanRLEDecoder{}, []decoderTestCase{
		{
			data:    []byte{0x02, 0x00, 0x00, 0x00, 0x03, 0x15},
			decoded: []interface{}{true, false, true, false, true},
		},
		{
			data:    []byte{0x02, 0x00, 0x00, 0x00, 0x10, 0x01},
			decoded: []interface{}{true, true, true, true, true, true, true, true},
		},
	})
}
