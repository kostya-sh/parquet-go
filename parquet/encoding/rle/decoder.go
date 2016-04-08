package rle

import (
	"bufio"
	"fmt"
	"io"
	"log"

	"github.com/kostya-sh/parquet-go/parquet/encoding/bitpacking"
)

func ReadVarint32(r io.ByteReader) (int32, error) {
	var (
		x     int32
		shift uint
		err   error
		b     byte
	)

	for {
		b, err = r.ReadByte()
		if err != nil {
			return x, err
		}
		x |= (int32(b&0x7f) << shift)
		shift += 7
		if (b & 0x80) == 0 {
			return x, nil
		}
	}
}

// type HybridBitPackingRLEEncoder struct {
// 	w      *bufio.Writer
// 	buffer []byte
// }

// func NewHybridBitPackingRLEEncoder(w io.Writer) *HybridBitPackingRLEEncoder {
// 	return &HybridBitPackingRLEEncoder{bufio.NewWriter(w), make([]byte, binary.MaxVarintLen64)}
// }

// func (e *HybridBitPackingRLEEncoder) Write(count uint32, value int32) (err error) {
// 	var (
// 		indicator int32 = int32(count<<1 | 0)
// 		i         int
// 	)

// 	i = binary.PutVarint(e.buffer, indicator)
// 	i, err = e.w.Write(e.buffer[:i])
// 	if err != nil {
// 		return err
// 	}

// 	err = binary.Write(e.w, binary.LittleEndian, value)
// 	return
// }

// ReadBool
func ReadBool(r io.Reader, count uint) ([]bool, error) {
	var out []bool
	bitWidth := uint(1) // fixed for booleans
	byteWidth := (bitWidth + uint(7)) / uint(8)
	p := make([]byte, byteWidth)

	br := bufio.NewReader(r)

	for {

		// run := <bit-packed-run> | <rle-run>
		header, err := ReadVarint32(br)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if (header & 1) == 1 {
			// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
			// we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
			// bit-pack-count := (number of values in this run) / 8
			literalCount := (header >> 1) * 8

			if uint(literalCount) > ((count - uint(len(out))) + 7) {
				return nil, fmt.Errorf("bitcoding.bool:bad encoding found more elements (%d) than expected (%d)", uint(len(out))+uint(literalCount), count)
			}

			r := bitpacking.NewDecoder(bitWidth)

			values := make([]int32, literalCount)

			if err := r.Read(br, values); err != nil {
				return nil, err
			}

			for i := int32(0); i < literalCount; i++ {
				out = append(out, values[i] == 1)
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

			if uint(repeatCount) > (count - uint(len(out))) {
				return nil, fmt.Errorf("rle.bool:bad encoding: found more elements (%d) than expected (%d)", uint(len(out))+uint(repeatCount), count)
			}

			for i := int32(0); i < repeatCount; i++ {
				out = append(out, value == 1)
			}
		}
	}

	if uint(len(out)) < count {
		return nil, fmt.Errorf("could not decode %d values only %d", count, len(out))
	}

	return out[:count], nil
}

// ReadInt32 .
func ReadInt32(r io.Reader, bitWidth uint, count uint) ([]int32, error) {
	var out []int32
	byteWidth := (bitWidth + uint(7)) / uint(8)
	p := make([]byte, byteWidth)

	br := bufio.NewReader(r)

	dec := bitpacking.NewDecoder(bitWidth)

	run := -1

	for {
		run++
		log.Printf("run %d:%v", run, out)
		// run := <bit-packed-run> | <rle-run>
		header, err := ReadVarint32(br)

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

			log.Printf("run %d (bitcoding):%d", run, literalCount)

			if int(literalCount) > (int(count)-len(out))+7 {
				return out, fmt.Errorf("bitcoding.int32: bad encoding: found more elements (%d) than expected (%d) run %d", uint(len(out))+uint(literalCount), count, run)
			}

			values := make([]int32, literalCount)

			if err := dec.Read(br, values); err != nil {
				return nil, err
			}

			for i := int32(0); i < literalCount; i++ {
				out = append(out, int32(values[i]))
			}

		} else {
			// rle-run := <rle-header> <repeated-value>
			// rle-header := varint-encode( (number of times repeated) << 1)
			// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
			repeatCount := int32(header >> 1)

			log.Printf("run %d (rle):%d", run, repeatCount)

			if _, err := br.Read(p); err != nil {
				return nil, fmt.Errorf("short read value: %s", err)
			}
			value := unpackLittleEndianInt32(p)

			if uint(repeatCount) > (count - uint(len(out))) {
				return nil, fmt.Errorf("rle.int32:bad encoding: found more elements (%d) than expected (%d)", uint(len(out))+uint(repeatCount), count)
			}

			for i := int32(0); i < repeatCount; i++ {
				out = append(out, int32(value))
			}

		}
	}

	if uint(len(out)) < count {
		return nil, fmt.Errorf("could not decode %d values only %d", count, len(out))
	}

	return out[:count], nil
}

// ReadInt32 .
func ReadUint32(r io.Reader, bitWidth uint, count uint) ([]uint32, error) {
	var out []uint32
	byteWidth := (bitWidth + uint(7)) / uint(8)
	p := make([]byte, byteWidth)

	br := bufio.NewReader(r)

	dec := bitpacking.NewDecoder(bitWidth)

	run := -1

	for {
		run++

		// run := <bit-packed-run> | <rle-run>
		header, err := ReadVarint32(br)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if (header & 1) == 1 {
			// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
			// we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
			// bit-pack-count := (number of values in this run) / 8
			literalCount := (header >> 1) * 8

			log.Printf("run %d (bitcoding):%d", run, literalCount)

			if uint(literalCount) > ((count - uint(len(out))) + 7) {
				return nil, fmt.Errorf("bitcoding.int32.bad encoding found more elements (%d) than expected (%d) run %d", uint(len(out))+uint(literalCount), count, run)
			}

			values := make([]int32, literalCount)

			if err := dec.Read(br, values); err != nil {
				return nil, err
			}

			for i := int32(0); i < literalCount; i++ {
				out = append(out, uint32(values[i]))
			}

		} else {
			// rle-run := <rle-header> <repeated-value>
			// rle-header := varint-encode( (number of times repeated) << 1)
			// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
			repeatCount := header >> 1

			log.Printf("run %d (rle):%d", run, repeatCount)

			if _, err := br.Read(p); err != nil {
				return nil, fmt.Errorf("short read value: %s", err)
			}
			value := unpackLittleEndianInt32(p)

			if uint(repeatCount) > (count - uint(len(out))) {
				return nil, fmt.Errorf("rle.int32:bad encoding: found more elements (%d) than expected (%d)", uint(len(out))+uint(repeatCount), count)
			}

			for i := int32(0); i < repeatCount; i++ {
				out = append(out, uint32(value))
			}
		}
	}

	log.Println("total values read: ", len(out), " of ", count)

	if uint(len(out)) < count {
		return nil, fmt.Errorf("could not decode %d values only %d", count, len(out))
	}

	return out[:count], nil
}

// func ReadUint32(r io.Reader, bitWidth uint, count uint) ([]uint32, error) {
// 	var out []uint32

// 	r = dump("uint32", r)

// 	byteWidth := (bitWidth + uint(7)) / uint(8)
// 	p := make([]byte, byteWidth)

// 	br := bufio.NewReader(r)
// 	bitdec := bitpacking.NewDecoder(bitWidth)
// 	run := 1

// 	for {
// 		run++
// 		// run := <bit-packed-run> | <rle-run>
// 		header, err := binary.ReadVarint(br)

// 		if err == io.EOF {
// 			break
// 		} else if err != nil {
// 			return nil, err
// 		}

// 		if (header & 1) == 1 {
// 			// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
// 			// we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
// 			// bit-pack-count := (number of values in this run) / 8
// 			literalCount := int32(header>>1) * 8

// 			if literalCount > (int32(count)-int32(len(out)))+7 {
// 				return nil, fmt.Errorf("bitcoding.uint32:bad encoding found more elements (%d) than expected (%d) run:%d out:%d %d", len(out)+int(literalCount), count, run, len(out), literalCount)
// 			}

// 			values := make([]int32, literalCount)

// 			if err := bitdec.Read(br, values); err != nil {
// 				return nil, err
// 			}

// 			for i := int32(0); i < literalCount; i++ {
// 				out = append(out, uint32(values[i]))
// 			}

// 		} else {
// 			// rle-run := <rle-header> <repeated-value>
// 			// rle-header := varint-encode( (number of times repeated) << 1)
// 			// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
// 			repeatCount := int32(header >> 1)
// 			if n, err := br.Read(p); err != nil {
// 				return nil, fmt.Errorf("short read value: %d:%s", n, err)
// 			}
// 			value := unpackLittleEndianInt32(p)

// 			if repeatCount > (int32(count) - int32(len(out))) {
// 				return nil, fmt.Errorf("rle.uint32:bad encoding: found more elements (%d, %d) than expected (%d) run:%d, data:%d", len(out), repeatCount, count, run, len(out))
// 			}

// 			for i := int32(0); i < repeatCount; i++ {
// 				out = append(out, uint32(value))
// 			}

// 		}
// 	}

// 	if uint(len(out)) < count {
// 		return nil, fmt.Errorf("could not decode %d values only %d", count, len(out))
// 	}

// 	return out[:count], nil
// }

// bitWidth returns number of bits required to represent any number less or
// equal to max.
// TODO: maybe replace int with uint64, return result as well
func bitWidth(max int) int {
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

func unpackLittleEndianInt32(bytes []byte) int32 {
	switch len(bytes) {
	case 1:
		return int32(bytes[0])
	case 2:
		return int32(bytes[1]) + int32(bytes[0])<<8
	case 3:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16
	case 4:
		return int32(bytes[0]) + int32(bytes[1])<<8 + int32(bytes[2])<<16 + int32(bytes[3])<<24
	default:
		panic("invalid argument: " + string(len(bytes)))
	}
}

func packLittleEndianInt32(bytes []byte, value int32) int {
	switch len(bytes) {
	case 1:
		bytes[0] = byte(value)
		return 1
	case 2:
		bytes[0] = byte(value)
		bytes[1] = byte(value >> 8)
		return 2
	case 3:
		bytes[0] = byte(value)
		bytes[1] = byte(value >> 8)
		bytes[2] = byte(value >> 16)
		return 3
	case 4:
		bytes[0] = byte(value)
		bytes[1] = byte(value >> 8)
		bytes[2] = byte(value >> 16)
		bytes[3] = byte(value >> 24)
		return 4
	default:
		panic("invalid argument: " + string(len(bytes)))
	}
}
