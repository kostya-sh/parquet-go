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
	d.init(data)
	c := rand.Intn(5) + 1 // use random capacity in attempt to increase test coverage
	buf := make([]interface{}, c, c)
	for {
		var n int
		n, err = d.decode(buf)
		if err != nil {
			return a, err
		}
		a = append(a, buf[0:n]...)
		if len(a) >= count {
			// valuesDecoder is not aware of the expected number of values some
			// of the implementations (such as booleanPlainDecoder) can read
			// more values than necessary
			return a[:count], nil
		}
	}
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
		// some decoders (such as booleanPlainDecoder) can return slightly more
		// values than was originally encoded so try decoding up to 10 values first
		_, err = d.decode(make([]interface{}, 10, 10))
		if err == nil {
			_, err = d.decode(make([]interface{}, 1, 1))
		}
		if err == nil {
			t.Errorf("error expected attempting to read too many values from %v", test.data)
		} else {
			t.Logf("%v: %s", test.data, err)
		}

	}
}
