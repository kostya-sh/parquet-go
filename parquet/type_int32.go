package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type int32Decoder interface {
	decodeInt32(dst []int32) error
}

func decodeInt32(d int32Decoder, dst interface{}) error {
	switch dst := dst.(type) {
	case []int32:
		return d.decodeInt32(dst)
	case []interface{}:
		b := make([]int32, len(dst), len(dst))
		err := d.decodeInt32(b)
		for i := 0; i < len(dst); i++ {
			dst[i] = b[i]
		}
		return err
	default:
		panic("invalid argument")
	}
}

type int32PlainDecoder struct {
	data []byte

	pos int
}

func (d *int32PlainDecoder) init(data []byte) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *int32PlainDecoder) decode(dst interface{}) error {
	return decodeInt32(d, dst)
}

func (d *int32PlainDecoder) decodeInt32(dst []int32) error {
	for i := 0; i < len(dst); i++ {
		if d.pos >= len(d.data) {
			return errNED
		}
		if uint(d.pos+4) > uint(len(d.data)) {
			return errors.New("int32/plain: not enough bytes to decode an int32 number")
		}
		dst[i] = int32(binary.LittleEndian.Uint32(d.data[d.pos:]))
		d.pos += 4
	}
	return nil
}

type int32DictDecoder struct {
	dictDecoder

	values []int32
}

func (d *int32DictDecoder) initValues(dictData []byte, count int) error {
	d.numValues = count
	d.values = make([]int32, count, count)
	return d.dictDecoder.initValues(d.values, dictData)
}

func (d *int32DictDecoder) decode(dst interface{}) error {
	return decodeInt32(d, dst)
}

func (d *int32DictDecoder) decodeInt32(dst []int32) error {
	keys, err := d.decodeKeys(len(dst))
	if err != nil {
		return err
	}
	for i, k := range keys {
		dst[i] = d.values[k]
	}
	return nil
}

type int32DeltaBinaryPackedDecoder struct {
	data []byte

	blockSize     int32
	miniBlocks    int32
	miniBlockSize int32
	numValues     int32

	minDelta        int32
	miniBlockWidths []byte

	pos             int
	i               int
	value           int32
	miniBlock       int
	miniBlockWidth  int
	unpacker        unpack8int32Func
	miniBlockPos    int
	miniBlockValues [8]int32
}

func (d *int32DeltaBinaryPackedDecoder) init(data []byte) error {
	d.data = data

	d.pos = 0
	d.i = 0

	if err := d.readPageHeader(); err != nil {
		return err
	}
	if err := d.readBlockHeader(); err != nil {
		return err
	}

	return nil
}

func (d *int32DeltaBinaryPackedDecoder) decode(dst interface{}) error {
	return decodeInt32(d, dst)
}

// page-header := <block size in values> <number of miniblocks in a block> <total value count> <first value>
func (d *int32DeltaBinaryPackedDecoder) readPageHeader() error {
	var n int

	d.blockSize, n = varInt32(d.data[d.pos:])
	if n <= 0 {
		return errors.New("int32/delta: failed to read block size")
	}
	if d.blockSize <= 0 {
		return errors.New("int32/delta: invalid block size")
	}
	d.pos += n // TODO: overflow (and below)

	d.miniBlocks, n = varInt32(d.data[d.pos:])
	if n <= 0 {
		return errors.New("int32/delta: failed to read number of mini blocks")
	}
	if d.miniBlocks <= 0 || d.miniBlocks > d.blockSize {
		return errors.New("int32/delta: invalid number of miniblocks in a block")
	}
	// TODO: valdiate max d.miniBlocks (?)
	// TODO: do not allocate if not necessary
	d.miniBlockWidths = make([]byte, d.miniBlocks, d.miniBlocks)
	d.pos += n

	d.miniBlockSize = d.blockSize / d.miniBlocks // TODO: rounding

	d.numValues, n = varInt32(d.data[d.pos:])
	if n <= 0 {
		return fmt.Errorf("int32/delta: failed to read total value count")
	}
	d.pos += n

	d.value, n = zigZagVarInt32(d.data[d.pos:])
	if n <= 0 {
		return fmt.Errorf("delta: failed to read first value")
	}
	d.pos += n

	return nil
}

// block := <min delta> <list of bitwidths of miniblocks> <miniblocks>
// min delta : zig-zag var int encoded
// bitWidthsOfMiniBlock : 1 byte little endian
func (d *int32DeltaBinaryPackedDecoder) readBlockHeader() error {
	var n int

	d.minDelta, n = zigZagVarInt32(d.data[d.pos:])
	if n <= 0 {
		return errors.New("int32/delta: failed to read min delta")
	}
	d.pos += n

	n = copy(d.miniBlockWidths, d.data[d.pos:])
	if n != len(d.miniBlockWidths) {
		return errors.New("int32/delta: failed to read all miniblock bit widths")
	}
	for _, w := range d.miniBlockWidths {
		if w < 0 || w > 32 {
			return errors.New("int32/delta: invalid miniblock bit width")
		}
	}
	d.pos += n

	d.miniBlock = 0

	return nil
}

func (d *int32DeltaBinaryPackedDecoder) decodeInt32(dst []int32) error {
	n := 0
	var err error
	for n < len(dst) && d.i < int(d.numValues) {
		if d.i%8 == 0 {
			if d.i%int(d.miniBlockSize) == 0 {
				if d.miniBlock >= int(d.miniBlocks) {
					err = d.readBlockHeader()
					if err != nil {
						return err
					}
				}

				d.miniBlockWidth = int(d.miniBlockWidths[d.miniBlock])
				d.unpacker = unpack8Int32FuncByWidth[d.miniBlockWidth]
				d.miniBlockPos = 0
				d.miniBlock++
			}
			w := int(d.miniBlockWidth)
			if d.pos+w > len(d.data) {
				return fmt.Errorf("int32/delta: not enough data")
			}
			d.miniBlockValues = d.unpacker(d.data[d.pos : d.pos+w]) // TODO: validate w
			d.miniBlockPos += w
			d.pos += w
			if d.i+8 >= int(d.numValues) {
				d.pos += int(d.miniBlockSize)/8*w - d.miniBlockPos
			}
		}
		dst[n] = d.value
		d.value += d.miniBlockValues[d.i%8] + d.minDelta
		d.i++
		n++

	}
	if n == 0 {
		return fmt.Errorf("int32/delta: no more data")
	}

	return nil
}
