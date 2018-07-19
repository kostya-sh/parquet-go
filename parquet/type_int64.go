package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type int64Decoder interface {
	decodeInt64(dst []int64) error
}

func decodeInt64(d int64Decoder, dst interface{}) error {
	switch dst := dst.(type) {
	case []int64:
		return d.decodeInt64(dst)
	case []interface{}:
		b := make([]int64, len(dst), len(dst))
		err := d.decodeInt64(b)
		for i := 0; i < len(dst); i++ {
			dst[i] = b[i]
		}
		return err
	default:
		panic("invalid argument")
	}
}

type int64PlainDecoder struct {
	data []byte
}

func (d *int64PlainDecoder) init(data []byte) error {
	d.data = data
	return nil
}

func (d *int64PlainDecoder) decode(dst interface{}) error {
	return decodeInt64(d, dst)
}

func (d *int64PlainDecoder) decodeInt64(dst []int64) error {
	for i := 0; i < len(dst); i++ {
		if len(d.data) == 0 {
			return errNED
		}
		if len(d.data) < 8 {
			return errors.New("int64/plain: not enough bytes to decode an int64 number")
		}
		dst[i] = int64(binary.LittleEndian.Uint64(d.data))
		d.data = d.data[8:]
	}
	return nil
}

type int64DictDecoder struct {
	dictDecoder

	values []int64
}

func (d *int64DictDecoder) initValues(dictData []byte, count int) error {
	d.numValues = count
	d.values = make([]int64, count, count)
	return d.dictDecoder.initValues(d.values, dictData)
}

func (d *int64DictDecoder) decode(dst interface{}) error {
	return decodeInt64(d, dst)
}

func (d *int64DictDecoder) decodeInt64(dst []int64) error {
	keys, err := d.decodeKeys(len(dst))
	if err != nil {
		return err
	}
	for i, k := range keys {
		dst[i] = d.values[k]
	}
	return nil
}

type int64DeltaBinaryPackedDecoder struct {
	data []byte

	numMiniBlocks int32
	miniBlockSize int32
	numValues     int32

	minDelta        int64
	miniBlockWidths []uint8

	i               int
	value           int64
	miniBlock       int
	miniBlockWidth  int
	unpacker        unpack8int64Func
	miniBlockPos    int
	miniBlockValues [8]int64
}

func (d *int64DeltaBinaryPackedDecoder) init(data []byte) error {
	d.data = data

	d.i = 0

	if err := d.readPageHeader(); err != nil {
		return err
	}
	if err := d.readBlockHeader(); err != nil {
		return err
	}

	return nil
}

func (d *int64DeltaBinaryPackedDecoder) decode(dst interface{}) error {
	return decodeInt64(d, dst)
}

// page-header := <block size in values> <number of miniblocks in a block> <total value count> <first value>
func (d *int64DeltaBinaryPackedDecoder) readPageHeader() error {
	blockSize, n := varInt32(d.data)
	if n <= 0 {
		return errors.New("int64/delta: failed to read block size")
	}
	if blockSize <= 0 {
		// TODO: maybe validate blockSize % 8 = 0
		return errors.New("int64/delta: invalid block size")
	}
	d.data = d.data[n:]

	d.numMiniBlocks, n = varInt32(d.data)
	if n <= 0 {
		return errors.New("int64/delta: failed to read number of mini blocks")
	}
	if d.numMiniBlocks <= 0 || d.numMiniBlocks > blockSize || blockSize%d.numMiniBlocks != 0 {
		// TODO: maybe blockSize/8 % d.numMiniBlocks = 0
		return errors.New("int64/delta: invalid number of mini blocks")
	}
	d.data = d.data[n:]

	d.numValues, n = varInt32(d.data)
	if n <= 0 {
		return fmt.Errorf("int64/delta: failed to read total value count")
	}
	if d.numValues < 0 {
		return errors.New("int64/delta: invalid total value count")
	}
	d.data = d.data[n:]

	d.value, n = zigZagVarInt64(d.data)
	if n <= 0 {
		return errors.New("int64/delta: failed to read first value")
	}
	d.data = d.data[n:]

	// TODO: re-use if possible
	d.miniBlockWidths = make([]byte, d.numMiniBlocks, d.numMiniBlocks)
	d.miniBlockSize = blockSize / d.numMiniBlocks

	return nil
}

// block := <min delta> <list of bitwidths of miniblocks> <miniblocks>
// min delta : zig-zag var int encoded
// bitWidthsOfMiniBlock : 1 byte little endian
func (d *int64DeltaBinaryPackedDecoder) readBlockHeader() error {
	var n int

	d.minDelta, n = zigZagVarInt64(d.data)
	if n <= 0 {
		return errors.New("int64/delta: failed to read min delta")
	}
	d.data = d.data[n:]

	n = len(d.miniBlockWidths)
	if len(d.data) < n {
		return errors.New("int64/delta: not enough data to read all miniblock bit widths")
	}
	for i := 0; i < n; i++ {
		w := uint8(d.data[i])
		if w < 0 || w > 64 {
			return errors.New("int64/delta: invalid miniblock bit width")
		}
		d.miniBlockWidths[i] = w
	}
	d.data = d.data[n:]

	d.miniBlock = 0

	return nil
}

func (d *int64DeltaBinaryPackedDecoder) decodeInt64(dst []int64) error {
	for i := 0; i < len(dst); i++ {
		if d.i >= int(d.numValues) {
			return errNED
		}
		if d.i%8 == 0 {
			if d.i%int(d.miniBlockSize) == 0 {
				if d.miniBlock >= int(d.numMiniBlocks) {
					if err := d.readBlockHeader(); err != nil {
						return err
					}
				}

				d.miniBlockWidth = int(d.miniBlockWidths[d.miniBlock])
				d.unpacker = unpack8Int64FuncByWidth[d.miniBlockWidth]
				d.miniBlockPos = 0
				d.miniBlock++
			}

			// read next 8 values
			w := d.miniBlockWidth
			if w > len(d.data) {
				return errors.New("int64/delta: not enough data to read 8 values")
			}
			d.miniBlockValues = d.unpacker(d.data[:w])
			d.miniBlockPos += w
			d.data = d.data[w:]
			if d.i+8 >= int(d.numValues) {
				// make sure that all data is consumed
				// this is needed for byte array decoders
				d.data = d.data[int(d.miniBlockSize)/8*w-d.miniBlockPos:]
			}
		}
		dst[i] = d.value
		d.value += d.miniBlockValues[d.i%8] + d.minDelta
		d.i++
	}

	return nil
}
