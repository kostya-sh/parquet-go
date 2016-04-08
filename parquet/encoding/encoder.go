package encoding

import (
	"io"

	"github.com/kostya-sh/parquet-go/parquet/datatypes"
)

// Encoder interface
type Encoder interface {
	WriteBool(io.Writer, []bool) (int, error)
	WriteInt32(io.Writer, []int32) error
	WriteInt64(io.Writer, []int64) error
	//WriteInt96([]int64, []int32) (count uint, err error)
	WriteFloat32(io.Writer, []float32) error
	WriteFloat64(io.Writer, []float64) error
	WriteByteArray(io.Writer, [][]byte) error
}

// Decoder interface
type Decoder interface {
	DecodeBool([]bool) (count uint, err error)
	DecodeInt32([]int32) (count uint, err error)
	DecodeInt64([]int64) (count uint, err error)
	DecodeInt96([]datatypes.Int96) (count uint, err error)
	DecodeByteArray([][]byte) (count uint, err error)
	DecodeFixedByteArray([][]byte, uint) (count uint, err error)
	DecodeFloat32([]float32) (count uint, err error)
	DecodeFloat64([]float64) (count uint, err error)
}
