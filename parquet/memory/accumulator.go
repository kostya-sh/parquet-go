package memory

import (
	"fmt"

	"github.com/kostya-sh/parquet-go/parquet/encoding"
	"github.com/kostya-sh/parquet-go/parquet/thrift"
)

type Accumulator interface {
	Accumulate(d encoding.Decoder, count uint) error
	Get(int) (interface{}, bool)
}

func NewSimpleAccumulator(e *thrift.SchemaElement) Accumulator {
	t := e.GetType()
	switch t {
	case thrift.Type_BOOLEAN:
		return new(boolAccumulator)
	case thrift.Type_INT32:
		return new(int32Accumulator)
	case thrift.Type_INT64:
		return new(int64Accumulator)
	case thrift.Type_INT96:
		return new(int64Accumulator)
	case thrift.Type_FLOAT:
		return new(float32Accumulator)
	case thrift.Type_DOUBLE:
		return new(float64Accumulator)
	case thrift.Type_BYTE_ARRAY:
		return new(byteAccumulator)
	case thrift.Type_FIXED_LEN_BYTE_ARRAY:
		return &byteAccumulator{}
	default:
		panic("unknown type " + t.String())
	}
}

type boolAccumulator struct {
	buff []bool
}

func (b *boolAccumulator) Accumulate(d encoding.Decoder, count uint) error {
	b.buff = make([]bool, count)

	read, err := d.DecodeBool(b.buff)
	if err != nil {
		return err
	}

	if read != count {
		return fmt.Errorf("could not read all the expected values (%d) only %d", count, read)
	}

	return nil
}

func (b *boolAccumulator) Get(i int) (interface{}, bool) {
	if i < len(b.buff) {
		return b.buff[i], true
	}
	return nil, false
}

type int64Accumulator struct {
	buff []int64
}

func (b *int64Accumulator) Accumulate(d encoding.Decoder, count uint) error {
	b.buff = make([]int64, count)

	read, err := d.DecodeInt64(b.buff)
	if err != nil {
		return err
	}

	if read != count {
		return fmt.Errorf("could not read all the expected values (%d) only %d", count, read)
	}

	return nil
}

func (b *int64Accumulator) Get(i int) (interface{}, bool) {
	if i < len(b.buff) {
		return b.buff[i], true
	}
	return nil, false
}

type int32Accumulator struct {
	buff []int32
}

func (b *int32Accumulator) Accumulate(d encoding.Decoder, count uint) error {
	if b.buff == nil {
		b.buff = make([]int32, count)
	}

	read, err := d.DecodeInt32(b.buff)
	if err != nil {
		return err
	}

	if read != count {
		return fmt.Errorf("could not read all the expected values (%d) only %d", count, read)
	}

	return nil
}

func (b *int32Accumulator) Get(i int) (interface{}, bool) {
	if i < len(b.buff) {
		return b.buff[i], true
	}
	return nil, false
}

// type stringAccumulator struct {
// 	buff []string
// }

// func (b *stringAccumulator) Accumulate(d encoding.Decoder, count uint) error {
// 	if b.buff == nil {
// 		b.buff = make([]string, count)
// 	}

// 	read, err := d.DecodeString(b.buff)
// 	if err != nil {
// 		return err
// 	}

// 	if read != count {
// 		return fmt.Errorf("could not read all the expected values (%d) only %d", count, read)
// 	}

// 	return nil
// }

type float32Accumulator struct {
	buff []float32
}

func (b *float32Accumulator) Accumulate(d encoding.Decoder, count uint) error {
	if b.buff == nil {
		b.buff = make([]float32, count)
	}

	read, err := d.DecodeFloat32(b.buff)
	if err != nil {
		return err
	}

	if read != count {
		return fmt.Errorf("%s: could not read all the expected values (%d) only %d", d, count, read)
	}

	return nil
}

func (b *float32Accumulator) Get(i int) (interface{}, bool) {
	if i < len(b.buff) {
		return b.buff[i], true
	}
	return nil, false
}

type float64Accumulator struct {
	buff []float64
}

func (b *float64Accumulator) Accumulate(d encoding.Decoder, count uint) error {
	b.buff = make([]float64, count)

	read, err := d.DecodeFloat64(b.buff)
	if err != nil {
		return err
	}

	if read != count {
		return fmt.Errorf("could not read all the expected values (%d) only %d", count, read)
	}

	return nil
}

func (b *float64Accumulator) Get(i int) (interface{}, bool) {
	if i < len(b.buff) {
		return b.buff[i], true
	}
	return nil, false
}

type byteAccumulator struct {
	buff [][]byte
	size int32
}

func (b *byteAccumulator) Accumulate(d encoding.Decoder, count uint) error {
	b.buff = make([][]byte, count)

	if b.size == 0 {
		read, err := d.DecodeByteArray(b.buff)
		if err != nil {
			return fmt.Errorf("decodeByteArray: %s", err)
		}
		if read != count {
			return fmt.Errorf("decodeByteArray: could not read all the expected values (%d) only %d", count, read)
		}
	} else {
		read, err := d.DecodeFixedByteArray(b.buff, uint(b.size))
		if err != nil {
			return fmt.Errorf("decodeFixedByteArray: %s", err)
		}
		if read != count {
			return fmt.Errorf("decodeFixedByteArray: could not read all the expected values (%d) only %d", count, read)
		}
	}

	return nil
}

func (b *byteAccumulator) Get(i int) (interface{}, bool) {
	if i < len(b.buff) {
		return string(b.buff[i]), true // FIXME: temporary
	}
	return nil, false
}
