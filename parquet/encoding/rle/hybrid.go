package rle

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
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
	rb *bufio.Reader
}

// NewHybridBitPackingRLEDecoder
func NewHybridBitPackingRLEDecoder(r io.Reader) *HybridBitPackingRLEDecoder {
	return &HybridBitPackingRLEDecoder{rb: bufio.NewReader(r)}
}

// Scan
func (d *HybridBitPackingRLEDecoder) Read(out []uint64, bitWidth uint) error {
	rb := d.rb

	// length of the <encoded-data> in bytes stored as 4 bytes little endian

	// var length uint32

	// if err := binary.Read(rb, binary.LittleEndian, &length); err != nil {
	// 	return err
	// }

	length, err := binary.ReadVarint(rb)
	if err != nil {
		return err
	}

	if length == 0 || length > 1024 {
		return fmt.Errorf("invalid length: %d ", length)
	}

	log.Printf("length: %d", length)

	lr := io.LimitReader(rb, int64(length))

	rb = bufio.NewReader(lr)

	// run := <bit-packed-run> | <rle-run>
	header, err := binary.ReadVarint(rb)

	if err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}

	r := newBitReader(rb)

	if (header & 1) == 1 {
		// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
		// we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
		// bit-pack-count := (number of values in this run) / 8
		numValues := int(header>>1) * 8

		log.Printf("num Values: %d\n", numValues)

		for i := 0; i < numValues; i++ {
			value := r.ReadBits64(bitWidth)
			if value == 0 && r.Err() != nil {
				return r.Err()
			}
			log.Printf("%d %#v i:%d \n", len(out), out, i)
			out[i] = value
		}

	} else {
		// rle-run := <rle-header> <repeated-value>
		// rle-header := varint-encode( (number of times repeated) << 1)
		// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
		repeatCount := int(header >> 1)
		value := r.ReadBits64(bitWidth)
		if value == 0 && r.Err() != nil {
			return r.Err()
		}

		log.Printf("repeatCount: %d\n", repeatCount)

		for i := 0; i < repeatCount; i++ {
			out[i] = value
			log.Printf("%d %#v i:%d \n", len(out), out, i)
		}

	}

	return nil
}
