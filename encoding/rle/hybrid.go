package rle

import (
	"bufio"
	"encoding/binary"
	"io"
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
