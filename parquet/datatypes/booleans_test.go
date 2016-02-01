package datatypes

import (
	"math"
	"reflect"
	"testing"
)

func booleanPlainDecodeAll(d *booleanPlainDecoder, data []byte, count int) (a []bool, err error) {
	d.init(data)
	for i := 0; i < count; i++ {
		var next bool
		next, err = d.next()
		if err != nil {
			return
		}
		a = append(a, next)
	}
	return
}

func TestBooleanPlainDecoder(t *testing.T) {
	tests := []struct {
		data    []byte
		decoded []bool
	}{
		{
			data:    []byte{0x00},
			decoded: []bool{false, false, false, false, false},
		},
		{
			data:    []byte{0xFF},
			decoded: []bool{true, true, true},
		},
		{
			data:    []byte{0x06E}, // 0b01101110
			decoded: []bool{false, true, true, true, false, true, true, false},
		},
		{
			data:    []byte{0xFF, 0x06E}, // 0b11111111 0b01101110
			decoded: []bool{true, true, true, true, true, true, true, true, false, true, true, true, false, true, true},
		},
	}

	d := newBooleanPlainDecoder()
	for _, test := range tests {
		values, err := booleanPlainDecodeAll(d, test.data, len(test.decoded))
		if err != nil {
			t.Errorf("unexpected error %s decoding %v", err, test.data)
			continue
		}
		if !reflect.DeepEqual(values, test.decoded) {
			t.Errorf("decoded %v into %v; want: %v", test.data, values, test.decoded)
		}

		// make sure that reading past data returns error
		values, err = booleanPlainDecodeAll(d, test.data, math.MaxInt32)
		if err == nil {
			t.Errorf("error expected attempting to read too many values from %v", test.data)
		} else {
			t.Logf("%v: %s", test.data, err)
		}
	}
}
