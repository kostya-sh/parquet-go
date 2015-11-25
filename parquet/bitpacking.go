package parquet

// Encoding/decoding bit-packed int32

// The values are packed from the LSB of each byte to the MSB, though the order
// of the bits in each value remains in the usual order of MSB to LSB.

// For example, to pack the same values as the example in the deprecated
// encoding above:

// The numbers 1 through 7 using bit width 3:
//
//   dec value: 0   1   2   3   4   5   6   7
//   bit value: 000 001 010 011 100 101 110 111
//   bit label: ABC DEF GHI JKL MNO PQR STU VWX
//
// would be encoded like this where spaces mark byte boundaries (3 bytes):
//
//   bit value: 10001000 11000110 11111010
//   bit label: HIDEFABC RMNOJKLG VWXSTUPQ

type unpack8int32Func func(data []byte) [8]int32

// returns function that can be used to unpack bit packed data using bit width w
// TODO: better name
func unpack8Int32FuncForWidth(w int) unpack8int32Func {
	// TODO: use static array of functions instead of switch
	switch w {
	case 1:
		return unpack8int32_1
	case 2:
		return unpack8int32_2
	case 3:
		return unpack8int32_3
	default:
		// TODO: support width from 4 to 32
		panic("nyi")
	}
}

func unpack8int32_1(data []byte) (a [8]int32) {
	a[0] = int32((data[0] >> 0) & 1)
	a[1] = int32((data[0] >> 1) & 1)
	a[2] = int32((data[0] >> 2) & 1)
	a[3] = int32((data[0] >> 3) & 1)
	a[4] = int32((data[0] >> 4) & 1)
	a[5] = int32((data[0] >> 5) & 1)
	a[6] = int32((data[0] >> 6) & 1)
	a[7] = int32((data[0] >> 7) & 1)
	return
}

func unpack8int32_2(data []byte) (a [8]int32) {
	a[0] = int32((data[0] >> 0) & 3)
	a[1] = int32((data[0] >> 2) & 3)
	a[2] = int32((data[0] >> 4) & 3)
	a[3] = int32((data[0] >> 6) & 3)
	a[4] = int32((data[1] >> 0) & 3)
	a[5] = int32((data[1] >> 2) & 3)
	a[6] = int32((data[1] >> 4) & 3)
	a[7] = int32((data[1] >> 6) & 3)
	return
}

func unpack8int32_3(data []byte) (a [8]int32) {
	a[0] = int32((data[0] >> 0) & 7)
	a[1] = int32((data[0] >> 3) & 7)
	a[2] = int32((data[0]>>6)&7 | (data[0]<<2)&7)
	a[3] = int32((data[1] >> 1) & 7)
	a[4] = int32((data[1] >> 4) & 7)
	a[5] = int32((data[1]>>7)&7 | (data[2]<<1)&7)
	a[6] = int32((data[2] >> 2) & 7)
	a[7] = int32((data[2] >> 5) & 7)
	return
}
