package parquet

import (
	"encoding/binary"
	"math"
)

func zigZagVarInt32(bytes []byte) (int32, int) {
	v, n := binary.Varint(bytes)
	if n <= 0 {
		return 0, n
	}
	if v > math.MaxInt32 || v < math.MinInt32 {
		return 0, -n
	}
	return int32(v), n
}

func zigZagVarInt64(bytes []byte) (int64, int) {
	return binary.Varint(bytes)
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
