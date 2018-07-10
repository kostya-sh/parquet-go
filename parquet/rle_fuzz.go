// +build gofuzz

package parquet

import (
	"fmt"
	"math/bits"
)

func FuzzRLE(data []byte) int {
	const maxCount = 10000

	r := 0
widthLoop:
	for w := 1; w <= 32; w++ { // TODO: from 1 or 0
		d := newRLEDecoder(w)
		d.init(data)

		for i := 0; i < maxCount; i++ {
			v, err := d.next()
			if err != nil {
				if err == errNED {
					r = 1
				}
				continue widthLoop
			}
			if bits.LeadingZeros32(uint32(v)) < 32-w {
				panic(fmt.Sprintf("decoded value %d is too large for width %d", v, w))
			}
		}
		r = 1
	}
	return r
}
