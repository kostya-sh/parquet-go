package parquet

import (
	"fmt"
	"io"
)

type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

type countingReader struct {
	rs io.ReadSeeker
	n  int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.rs.Read(p)
	r.n += int64(n)
	return
}

type accumulator struct {
	boolBuff      []bool
	int32Buff     []int32
	int64Buff     []int64
	float32Buff   []float32
	float64Buff   []float64
	byteArrayBuff [][]byte
}

func (a *accumulator) WriteInt32(value interface{}) error {
	switch v := value.(type) {
	case int32:
		a.int32Buff = append(a.int32Buff, v)
	default:
		return fmt.Errorf("invalid value expected int32")
	}
	return nil
}

func (a *accumulator) WriteInt64(value interface{}) error {
	switch v := value.(type) {
	case int64:
		a.int64Buff = append(a.int64Buff, v)
	default:
		return fmt.Errorf("invalid value expected int64")
	}
	return nil
}

func (a *accumulator) WriteFloat32(value interface{}) error {
	switch v := value.(type) {
	case float32:
		a.float32Buff = append(a.float32Buff, v)
	default:
		return fmt.Errorf("invalid value expected float32")
	}
	return nil
}

func (a *accumulator) WriteFloat64(value interface{}) error {
	switch v := value.(type) {
	case float64:
		a.float64Buff = append(a.float64Buff, v)
	default:
		return fmt.Errorf("invalid value expected float64")
	}
	return nil
}

func (a *accumulator) WriteBoolean(value interface{}) error {
	switch v := value.(type) {
	case bool:
		a.boolBuff = append(a.boolBuff, v)
	default:
		return fmt.Errorf("invalid value expected bool")
	}
	return nil
}

func (a *accumulator) WriteString(value interface{}) error {
	switch v := value.(type) {
	case string:
		a.byteArrayBuff = append(a.byteArrayBuff, []byte(v))
	case []byte:
		a.byteArrayBuff = append(a.byteArrayBuff, v)
	default:
		return fmt.Errorf("invalid value expected bool")
	}
	return nil
}

// CountingWriter counts the number of bytes written to it.
type CountingWriter struct {
	W io.Writer // underlying writer
	N int64     // total # of bytes written
}

// CountingWriter wraps an existing io.Writer
func NewCountingWriter(w io.Writer) *CountingWriter {
	return &CountingWriter{W: w, N: 0}
}

// Write implements the io.Writer interface.
func (wc *CountingWriter) Write(p []byte) (int, error) {
	n, err := wc.W.Write(p)
	wc.N += int64(n)
	return n, err
}

type WriteOffsetter interface {
	io.Writer
	Offset() int64
}
