package parquet

import (
	"testing"
)

func TestInt32PlainDecoder(t *testing.T) {
	testValuesDecoder(t, &int32PlainDecoder{}, []decoderTestCase{
		{
			data: []byte{
				0x00, 0x00, 0x00, 0x80,
				0xFF, 0xFF, 0xFF, 0x7F,
				0x00, 0x00, 0x00, 0x00,
				0x9C, 0xFF, 0xFF, 0xFF,
				0xEA, 0x00, 0x00, 0x00,
			},
			decoded: []interface{}{
				int32(-2147483648),
				int32(2147483647),
				int32(0),
				int32(-100),
				int32(234),
			},
		},
	})
}
