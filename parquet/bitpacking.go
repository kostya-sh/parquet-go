package parquet

import (
	"encoding/binary"
	"math"
)

//go:generate go run bitpacking_gen.go

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

func unpack8int32_0(data []byte) (a [8]int32) {
	_ = a[7]
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

// bitWidth returns number of bits required to represent any number less or
// equal to max.
// TODO: maybe replace int with uint64, return result as well
func bitWidth(max int) int {
	if max < 0 {
		panic("max should be >=0")
	}
	w := 0
	for max != 0 {
		w++
		max >>= 1
	}
	return w
}

// TODO: int32 or uint32?
func unpackLittleEndianInt32(bytes []byte) int32 {
	switch len(bytes) {
	case 1:
		return int32(bytes[0])
	case 2:
		return int32(bytes[0]) + int32(bytes[1])<<8
	case 3:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16
	case 4:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16 + int32(bytes[3])<<24
	default:
		panic("invalid argument")
	}
}

func zigZagVarInt32(bytes []byte) (int32, int) {
	uv, n := binary.Uvarint(bytes)
	if n <= 0 {
		return 0, n
	}
	if uv > math.MaxUint32 {
		return 0, -n
	}

	v := int32(uv / 2)
	if uv%2 == 0 {
		return v, n
	}
	return -v - 1, n
}

func varInt32(bytes []byte) (int32, int) {
	uv, n := binary.Uvarint(bytes)
	if n <= 0 {
		return 0, n
	}
	if uv > math.MaxInt32 {
		return 0, -n
	}
	return int32(uv), n
}
