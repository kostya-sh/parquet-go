package rle

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kostya-sh/parquet-go/parquet/encoding/bitpacking"
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
		indicator int64 = int64(count<<1 | 0)
		i         int
	)

	i = binary.PutVarint(e.buffer, indicator)
	i, err = e.w.Write(e.buffer[:i])
	if err != nil {
		return err
	}

	err = binary.Write(e.w, binary.LittleEndian, value)
	return
}

// ReadInt64 .
func ReadInt64(r io.Reader, bitWidth uint, count uint) ([]int64, error) {
	var out []int64

	byteWidth := (bitWidth + uint(7)) / uint(8)
	p := make([]byte, byteWidth)

	// r = dump(r)

	br := bufio.NewReader(r)

	for {

		// run := <bit-packed-run> | <rle-run>
		header, err := binary.ReadVarint(br)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if (header & 1) == 1 {
			// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
			// we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
			// bit-pack-count := (number of values in this run) / 8
			literalCount := int32(header>>1) * 8

			r := bitpacking.NewDecoder(br, int(bitWidth))
			for i := int32(0); i < literalCount; i++ {
				if r.Scan() {
					out = append(out, r.Value())
				}
				if err := r.Err(); err != nil {
					return nil, err
				}
			}

		} else {
			// rle-run := <rle-header> <repeated-value>
			// rle-header := varint-encode( (number of times repeated) << 1)
			// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
			repeatCount := int32(header >> 1)
			if _, err := br.Read(p); err != nil {
				return nil, fmt.Errorf("short read value: %s", err)
			}
			value := unpackLittleEndianInt32(p)

			for i := int32(0); i < repeatCount; i++ {
				out = append(out, int64(value))
			}

		}
	}

	if uint(len(out)) < count {
		return nil, fmt.Errorf("could not decode %d values only %d", count, len(out))
	}

	return out[:count], nil
}

func ReadUint64(r io.Reader, bitWidth uint, count uint) ([]uint64, error) {
	var out []uint64

	byteWidth := (bitWidth + uint(7)) / uint(8)
	p := make([]byte, byteWidth)

	// r = dump(r)

	br := bufio.NewReader(r)

	for {

		// run := <bit-packed-run> | <rle-run>
		header, err := binary.ReadUvarint(br)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if (header & 1) == 1 {
			// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
			// we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
			// bit-pack-count := (number of values in this run) / 8
			literalCount := int32(header>>1) * 8

			r := bitpacking.NewDecoder(br, int(bitWidth))

			for i := int32(0); i < literalCount; i++ {
				if r.Scan() {
					out = append(out, uint64(r.Value()))
				}
				if err := r.Err(); err != nil {
					return nil, err
				}
			}

		} else {
			// rle-run := <rle-header> <repeated-value>
			// rle-header := varint-encode( (number of times repeated) << 1)
			// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
			repeatCount := uint32(header >> 1)
			if _, err := br.Read(p); err != nil {
				return nil, fmt.Errorf("short read value: %s", err)
			}
			value := unpackLittleEndianInt32(p)

			for i := uint32(0); i < repeatCount; i++ {
				out = append(out, uint64(value))
			}

		}
	}

	if uint(len(out)) < count {
		return nil, fmt.Errorf("could not decode %d values only %d", count, len(out))
	}

	return out[:count], nil
}

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
		panic("invalid argument: " + string(len(bytes)))
	}
}
