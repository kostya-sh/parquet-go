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

func TestInt96DictDecoder(t *testing.T) {
	d := &int96DictDecoder{
		dictDecoder: dictDecoder{vd: &int96PlainDecoder{}},
	}

	ent := Int96{0x00, 0x58, 0x47, 0xf8, 0xd, 0x00, 0x00, 0x00, 0x6c, 0x75, 0x25, 0x00}
	if err := d.initValues(ent[:], 1); err != nil {
		t.Fatalf("error in initValues: %s", err)
	}

	if err := d.init([]byte{0x00, 0x98, 0x06}); err != nil {
		t.Fatalf("error in init: %s", err)
	}

	dst := make([]Int96, 1)
	if err := d.decode(dst); err != nil {
		t.Fatalf("error in decode: %s", err)
	}

	if dst[0] != ent {
		t.Fatalf("expected %v to equal %v", dst[0], ent)
	}
}
