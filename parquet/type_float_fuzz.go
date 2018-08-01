// +build gofuzz

package parquet

import "fmt"
import "math"

func fuzzFloat(d valuesDecoder, data []byte, initCanFail bool) int {
	const maxSize = 10000

	err := d.init(data)
	if err != nil {
		if initCanFail {
			return 0
		}
		panic("unexpected error in init")
	}
	dst1 := make([]float32, maxSize)
	err = d.decode(dst1)
	if err != nil && err != errNED {
		return 0
	}

	err = d.init(data)
	if err != nil {
		panic("unexpected error in the 2nd init")
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
		a := float64(dst1[i])
		b := float64(dst2[i].(float32))
		if math.IsNaN(a) && math.IsNaN(b) {
			continue
		}
		if a != b {
			panic(fmt.Sprintf("different values at %d: %g != %g", i, a, b))
		}
	}

	return 1
}

func FuzzFloatPlain(data []byte) int {
	return fuzzFloat(&floatPlainDecoder{}, data, false)
}

func FuzzFloatDict(data []byte) int {
	dictData := []byte{
		0x01, 0x00, 0x00, 0x00, // min floar
		0x00, 0x00, 0xC0, 0x7F, // NaN
		0x00, 0x00, 0x00, 0x00, // 0.0
		0xFA, 0x3E, 0xC8, 0x42, // 100.123
		0xFF, 0xFF, 0x7F, 0x7F, // max float
		0x33, 0x33, 0xC8, 0x42, // 100.1
		0x66, 0x66, 0xC8, 0x42, // 100.2
		0x9A, 0x99, 0xC8, 0x42, // 100.3
	}

	d := &floatDictDecoder{
		dictDecoder: dictDecoder{vd: &floatPlainDecoder{}},
	}

	r := 0
	for i := 0; i < 16; i++ {
		if err := d.initValues(dictData, i); err != nil {
			break
		}
		println(i)
		if fuzzFloat(d, data, true) == 1 {
			r = 1
		}
	}
	return r
}
