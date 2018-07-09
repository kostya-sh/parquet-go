package parquet

import (
	"math/rand"
	"reflect"
	"testing"
)

type decoderTestCase struct {
	data    []byte
	decoded []interface{}
}

func decodeAllValues(d valuesDecoder, data []byte, count int) (a []interface{}, err error) {
	if err = d.init(data); err != nil {
		return nil, err
	}

	// read all data by calling decode() method multiple times
	c := rand.Intn(4) + 1 // use random capacity in an attempt to increase test coverage
	buf := make([]interface{}, c, c)
	for count > 0 {
		n := c
		if count < n {
			n = count
		}
		count -= n
		err = d.decode(buf[0:n])
		if err != nil {
			return a, err
		}
		a = append(a, buf[0:n]...)
	}
	return a, nil
}

func testValuesDecoder(t *testing.T, d valuesDecoder, tests []decoderTestCase) {
	t.Helper()
	for _, test := range tests {
		values, err := decodeAllValues(d, test.data, len(test.decoded))
		if err != nil {
			t.Errorf("unexpected error %s decoding %v", err, test.data)
			continue
		}
		if !reflect.DeepEqual(values, test.decoded) {
			t.Errorf("decoded %v into %v; want: %v", test.data, values, test.decoded)
		}

		// make sure that reading past data returns error
		err = d.decode(make([]interface{}, 1000, 1000))
		if err == nil {
			t.Errorf("error expected attempting to read too many values from %v", test.data)
		} else {
			t.Logf("%v: %s", test.data, err)
		}
	}
}
