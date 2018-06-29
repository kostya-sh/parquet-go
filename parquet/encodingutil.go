package parquet

import (
	"encoding/binary"
	"math"
)

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

func zigZagVarInt64(bytes []byte) (int64, int) {
	uv, n := binary.Uvarint(bytes)
	if n <= 0 {
		return 0, n
	}
	v := int64(uv / 2)
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
