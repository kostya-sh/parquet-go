package parquet

import (
	"encoding/binary"
	"math"
)

// bitWidth16 returns number of bits required to represent any number less or
// equal to max.
func bitWidth16(max uint16) int {
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
