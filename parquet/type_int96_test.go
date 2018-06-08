package parquet

import (
	"testing"
)

func TestInt96PlainDecoder(t *testing.T) {
	testValuesDecoder(t, &int96PlainDecoder{}, []decoderTestCase{
		{
			data:    []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0xC},
			decoded: []interface{}{Int96{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
		},
	})
}
