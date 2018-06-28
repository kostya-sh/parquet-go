package parquet

//go:generate go run bitpacking_gen.go

// Encoding/decoding bit-packed int32 and int64

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

func unpack8int32_0(data []byte) (a [8]int32) {
	a[0] = 0
	a[1] = 0
	a[2] = 0
	a[3] = 0
	a[4] = 0
	a[5] = 0
	a[6] = 0
	a[7] = 0
	return
}

var unpack8Int32FuncByWidth = [33]unpack8int32Func{
	unpack8int32_0,
	unpack8int32_1,
	unpack8int32_2,
	unpack8int32_3,
	unpack8int32_4,
	unpack8int32_5,
	unpack8int32_6,
	unpack8int32_7,
	unpack8int32_8,
	unpack8int32_9,
	unpack8int32_10,
	unpack8int32_11,
	unpack8int32_12,
	unpack8int32_13,
	unpack8int32_14,
	unpack8int32_15,
	unpack8int32_16,
	unpack8int32_17,
	unpack8int32_18,
	unpack8int32_19,
	unpack8int32_20,
	unpack8int32_21,
	unpack8int32_22,
	unpack8int32_23,
	unpack8int32_24,
	unpack8int32_25,
	unpack8int32_26,
	unpack8int32_27,
	unpack8int32_28,
	unpack8int32_29,
	unpack8int32_30,
	unpack8int32_31,
	unpack8int32_32,
}

type unpack8int64Func func(data []byte) [8]int64

func unpack8int64_0(data []byte) (a [8]int64) {
	a[0] = 0
	a[1] = 0
	a[2] = 0
	a[3] = 0
	a[4] = 0
	a[5] = 0
	a[6] = 0
	a[7] = 0
	return
}

var unpack8Int64FuncByWidth = [65]unpack8int64Func{
	unpack8int64_0,
	unpack8int64_1,
	unpack8int64_2,
	unpack8int64_3,
	unpack8int64_4,
	unpack8int64_5,
	unpack8int64_6,
	unpack8int64_7,
	unpack8int64_8,
	unpack8int64_9,
	unpack8int64_10,
	unpack8int64_11,
	unpack8int64_12,
	unpack8int64_13,
	unpack8int64_14,
	unpack8int64_15,
	unpack8int64_16,
	unpack8int64_17,
	unpack8int64_18,
	unpack8int64_19,
	unpack8int64_20,
	unpack8int64_21,
	unpack8int64_22,
	unpack8int64_23,
	unpack8int64_24,
	unpack8int64_25,
	unpack8int64_26,
	unpack8int64_27,
	unpack8int64_28,
	unpack8int64_29,
	unpack8int64_30,
	unpack8int64_31,
	unpack8int64_32,
	unpack8int64_33,
	unpack8int64_34,
	unpack8int64_35,
	unpack8int64_36,
	unpack8int64_37,
	unpack8int64_38,
	unpack8int64_39,
	unpack8int64_40,
	unpack8int64_41,
	unpack8int64_42,
	unpack8int64_43,
	unpack8int64_44,
	unpack8int64_45,
	unpack8int64_46,
	unpack8int64_47,
	unpack8int64_48,
	unpack8int64_49,
	unpack8int64_50,
	unpack8int64_51,
	unpack8int64_52,
	unpack8int64_53,
	unpack8int64_54,
	unpack8int64_55,
	unpack8int64_56,
	unpack8int64_57,
	unpack8int64_58,
	unpack8int64_59,
	unpack8int64_60,
	unpack8int64_61,
	unpack8int64_62,
	unpack8int64_63,
	unpack8int64_64,
}
