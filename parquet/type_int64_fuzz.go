// +build gofuzz

package parquet

import "fmt"

func fuzzInt64(d valuesDecoder, data []byte, initCanFail bool) int {
	const maxSize = 10000

	err := d.init(data)
	if err != nil {
		if initCanFail {
			return 0
		}
		panic("unexpected error in init")
	}
	dst1 := make([]int64, maxSize)
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
		if dst1[i] != dst2[i].(int64) {
			panic(fmt.Sprintf("different values at %d: %d != %d", i, dst1[i], dst2[i]))
		}
	}

	return 1
}

func FuzzInt64Plain(data []byte) int {
	return fuzzInt64(&int64PlainDecoder{}, data, false)
}

func FuzzInt64DeltaBinaryPacked(data []byte) int {
	return fuzzInt64(&int64DeltaBinaryPackedDecoder{}, data, true)
}

func FuzzInt64Dict(data []byte) int {
	dictData := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x9C, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xEA, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}

	d := &int64DictDecoder{
		dictDecoder: dictDecoder{vd: &int64PlainDecoder{}},
	}

	r := 0
	for i := 0; i < 16; i++ {
		if err := d.initValues(dictData, i); err != nil {
			break
		}
		if fuzzInt64(d, data, true) == 1 {
			r = 1
		}
	}
	return r
}
