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

func TestEmptyFloatDictDecoder(t *testing.T) {
	d := &floatDictDecoder{
		dictDecoder: dictDecoder{vd: &floatPlainDecoder{}},
	}

	if err := d.initValues([]byte{}, 0); err != nil {
		t.Fatalf("Error in initValues: %s", err)
	}

	// test case found with go-fuzz
	if err := d.init([]byte{0x00, 0x30}); err != nil {
		t.Fatalf("error in init: %s", err)
	}
	if err := d.decode(make([]float32, 1)); err == nil {
		t.Errorf("error expected when decoding from a dictionary with no values")
	}
}

func TestFloatDictDecoderErrors(t *testing.T) {
	d := &floatDictDecoder{
		dictDecoder: dictDecoder{vd: &floatPlainDecoder{}},
	}

	if err := d.initValues([]byte{0x00, 0x00, 0x00, 0x00}, 1); err != nil {
		t.Fatalf("Error in initValues: %s", err)
	}

	// test case found with go-fuzz
	if err := d.init([]byte{0x00}); err == nil {
		t.Errorf("error expected in init (bit width = 0 for non-empty dictionary)")
	}
}
