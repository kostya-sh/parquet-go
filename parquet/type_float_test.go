package parquet

import (
	"math"
	"testing"
)

func TestFloatPlainDecoder(t *testing.T) {
	testValuesDecoder(t, &floatPlainDecoder{}, []decoderTestCase{
		{
			data: []byte{
				// 0x00, 0x00, 0xC0, 0x7F,
				0x00, 0x00, 0x80, 0xFF,
				0x00, 0x00, 0x80, 0x7F,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x80, 0x3F,
				0x00, 0x00, 0x80, 0xBF,
			},
			decoded: []interface{}{
				// float32(math.NaN()),
				float32(math.Inf(-1)),
				float32(math.Inf(+1)),
				float32(0.0),
				float32(1.0),
				float32(-1.0),
			},
		},
	})
}
