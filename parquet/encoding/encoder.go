package encoding

import "io"

// Encoder
type Encoder interface {
	WriteBool(io.Writer, []bool) error
	WriteInt32(io.Writer, []int32) error
	WriteInt64(io.Writer, []int64) error
	WriteFloat32(io.Writer, []float32) error
	WriteFloat64(io.Writer, []float64) error
	WriteByteArray(io.Writer, [][]byte) error
}
