// +build gofuzz

package parquet

import "fmt"

func fuzzBoolean(d valuesDecoder, data []byte) int {
	const maxSize = 10000

	err := d.init(data)
	if err != nil {
		return 0
	}
	dst1 := make([]bool, maxSize)
	err = d.decode(dst1)
	if err != nil && err != errNED {
		return 0
	}

	err = d.init(data)
	if err != nil {
		panic("unexpected error in the second init: " + err.Error())
	}
	dst2 := make([]interface{}, maxSize)
	err = d.decode(dst2)
	if err != nil && err != errNED {
		return 0
	}

	for i := 0; i < maxSize; i++ {
		if dst2[i] == nil {
			break
		}
		if dst1[i] != dst2[i].(bool) {
			panic(fmt.Sprintf("different values at %d", i))
		}
	}

	return 1
}

func FuzzBooleanPlain(data []byte) int {
	return fuzzBoolean(&booleanPlainDecoder{}, data)
}

func FuzzBooleanRLE(data []byte) int {
	return fuzzBoolean(&booleanRLEDecoder{}, data)
}
