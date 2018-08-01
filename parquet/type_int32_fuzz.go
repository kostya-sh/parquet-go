// +build gofuzz

package parquet

import "fmt"

func fuzzInt32(d valuesDecoder, data []byte, initCanFail bool) int {
	const maxSize = 10000

	err := d.init(data)
	if err != nil {
		if initCanFail {
			return 0
		}
		panic("unexpected error in init")
	}
	dst1 := make([]int32, maxSize)
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
		if dst1[i] != dst2[i].(int32) {
			panic(fmt.Sprintf("different values at %d: %d != %d", i, dst1[i], dst2[i]))
		}
	}

	return 1
}

func FuzzInt32Plain(data []byte) int {
	return fuzzInt32(&int32PlainDecoder{}, data, false)
}

func FuzzInt32DeltaBinaryPacked(data []byte) int {
	return fuzzInt32(&int32DeltaBinaryPackedDecoder{}, data, true)
}

func FuzzInt32Dict(data []byte) int {
	dictData := []byte{
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0xC0, 0x7F,
		0x00, 0x00, 0x00, 0x00,
		0xFA, 0x3E, 0xC8, 0x42,
		0xFF, 0xFF, 0x7F, 0x7F,
		0x33, 0x33, 0xC8, 0x42,
		0x66, 0x66, 0xC8, 0x42,
		0x9A, 0x99, 0xC8, 0x42,
	}

	d := &int32DictDecoder{
		dictDecoder: dictDecoder{vd: &int32PlainDecoder{}},
	}

	r := 0
	for i := 0; i < 16; i++ {
		if err := d.initValues(dictData, i); err != nil {
			break
		}
		println(i)
		if fuzzInt32(d, data, true) == 1 {
			r = 1
		}
	}
	return r
}
