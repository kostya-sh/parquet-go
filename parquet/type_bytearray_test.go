package parquet

import (
	"testing"
)

func TestByteArrayPlainDecoder(t *testing.T) {
	testValuesDecoder(t, &byteArrayPlainDecoder{}, []decoderTestCase{
		{
			data: []byte{
				0x03, 0x00, 0x00, 0x00, 0x31, 0x32, 0x33,
				0x00, 0x00, 0x00, 0x00,
				0x06, 0x00, 0x00, 0x00, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
			},
			decoded: []interface{}{
				[]byte("123"),
				[]byte(""),
				[]byte("ABCDEF"),
			},
		},
	})
}

func TestFixedLenByteArrayPlainDecoder(t *testing.T) {
	testValuesDecoder(t, &byteArrayPlainDecoder{length: 3}, []decoderTestCase{
		{
			data: []byte{
				0x31, 0x32, 0x33,
				0x41, 0x42, 0x43,
				0x44, 0x45, 0x46,
			},
			decoded: []interface{}{
				[]byte("123"),
				[]byte("ABC"),
				[]byte("DEF"),
			},
		},
	})
}
