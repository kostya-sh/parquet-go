package rle

import "io"

func WriteBool(w io.Writer, v []bool) error {

	// var lastValue bool
	// var count int

	// btpack := bitpacking.NewEncoder(1, bitpacking.RLE)

	// FIXME
	// for i, value := range v {
	// 	if i == 0 {
	// 		lastValue = value
	// 		continue
	// 	}

	// 	if lastValue == value {
	// 		count++
	// 	} else {
	// 		// if count is less than 8 use bit-packing
	// 		if value {
	// 			btpack.Write(1)
	// 		} else {
	// 			btpack.Write(0)
	// 		}
	// 	}
	// }

	return nil
}

func WriteInt64(w io.Writer, bitWidth uint, count uint) ([]int64, error) {
	return nil, nil
}

func WriteInt32(w io.Writer, bitWidth uint, count uint) ([]int64, error) {
	return nil, nil
}
