// +build gofuzz

package parquet

import "fmt"
import "math"

func FuzzFloatPlain(data []byte) int {
	const maxSize = 10000

	d := floatPlainDecoder{}
	err := d.init(data)
	if err != nil {
		panic("unexpected error in init")
	}
	dst1 := make([]float32, maxSize, maxSize)
	err = d.decode(dst1)
	if err != nil && err != errNED {
		return 0
	}

	err = d.init(data)
	if err != nil {
		panic("unexpected error in init")
	}
	dst2 := make([]interface{}, maxSize, maxSize)
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
