// +build gofuzz

package parquet

import "fmt"
import "math"

func FuzzDoublePlain(data []byte) int {
	const maxSize = 10000

	d := doublePlainDecoder{}
	err := d.init(data)
	if err != nil {
		panic("unexpected error in init")
	}
	dst1 := make([]float64, maxSize)
	err = d.decode(dst1)
	if err != nil && err != errNED {
		return 0
	}

	err = d.init(data)
	if err != nil {
		panic("unexpected error in init")
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
		a := dst1[i]
		b := dst2[i].(float64)
		if math.IsNaN(a) && math.IsNaN(b) {
			continue
		}
		if a != b {
			panic(fmt.Sprintf("different values at %d: %g != %g", i, a, b))
		}
	}

	return 1
}
