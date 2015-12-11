package parquet

import (
	"reflect"
	"testing"
)

func TestBooleanPlainDecoder(t *testing.T) {
	tests := []struct {
		data      []byte
		numValues int32
		decoded   []bool
	}{
		{
			data:      []byte{0x00},
			numValues: 5,
			decoded:   []bool{false, false, false, false, false},
		},
		{
			data:      []byte{0xFF},
			numValues: 3,
			decoded:   []bool{true, true, true},
		},
		{
			data:      []byte{0x06E}, // 0b01101110
			numValues: 8,
			decoded:   []bool{false, true, true, true, false, true, true, false},
		},
		{
			data:      []byte{0xFF, 0x06E}, // 0b11111111 0b01101110
			numValues: 15,
			decoded:   []bool{true, true, true, true, true, true, true, true, false, true, true, true, false, true, true},
		},
	}

	var d booleanPlainDecoder
	for _, test := range tests {
		d.init(test.data, test.numValues)
		var got []bool
		for d.next() {
			got = append(got, d.value)
		}
		if !reflect.DeepEqual(got, test.decoded) {
			t.Errorf("Wrong result for data=%v, numValues=%d. Expected: %v, got: %v",
				test.data, test.numValues, test.decoded, got)
		}
	}

}
