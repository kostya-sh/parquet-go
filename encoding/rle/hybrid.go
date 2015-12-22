package rle

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kostya-sh/parquet-go/encoding/bitpacking"
)

type HybridBitPackingRLEEncoder struct {
	w      *bufio.Writer
	buffer []byte
}

func NewHybridBitPackingRLEEncoder(w io.Writer) *HybridBitPackingRLEEncoder {
	return &HybridBitPackingRLEEncoder{bufio.NewWriter(w), make([]byte, binary.MaxVarintLen64)}
}

func (e *HybridBitPackingRLEEncoder) Write(count uint64, value int64) (err error) {
	var (
		indicator uint64 = count<<1 | 0
		i         int
	)

	i = binary.PutUvarint(e.buffer, indicator)
	i, err = e.w.Write(e.buffer[:i])
	if err != nil {
		return err
	}

	err = binary.Write(e.w, binary.LittleEndian, value)
	return
}

// HybridBitPackingRLEDecoder
type HybridBitPackingRLEDecoder struct {
	rb       *bufio.Reader
	bitWidth int
	err      error
	scanner  interface {
		Scan() bool
		Err() error
		Value() int64
	}
}

// NewHybridBitPackingRLEDecoder
func NewHybridBitPackingRLEDecoder(r io.Reader, bitWidth int) *HybridBitPackingRLEDecoder {
	return &HybridBitPackingRLEDecoder{rb: bufio.NewReader(r), bitWidth: bitWidth}
}

// Scan
func (d *HybridBitPackingRLEDecoder) Scan() bool {
	rb := d.rb

	if d.scanner == nil {
		// read count
		length, err := binary.ReadUvarint(rb)
		if err != nil {
			d.setErr(err)
			return false
		}

		if length == 0 {
			d.setErr(fmt.Errorf("invalid length"))
			return false
		}

		isLiteral := length & 1

		if isLiteral == 1 {
			byteCount := (length >> 1) * 8
			rb := io.LimitReader(rb, int64(byteCount))
			d.scanner = bitpacking.NewDecoder(rb, d.bitWidth)

		} else {
			//			repeatCount := length >> 1
			d.scanner = NewDecoder(rb)
		}
	}

	return d.scanner.Scan()
}

func (d *HybridBitPackingRLEDecoder) Err() error {
	return d.scanner.Err()
}

func (d *HybridBitPackingRLEDecoder) Value() int64 {
	return d.scanner.Value()
}

func (d *HybridBitPackingRLEDecoder) setErr(err error) {
	if d.err == nil || d.err == io.EOF {
		d.err = err
	}
}
