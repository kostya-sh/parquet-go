package parquet

import (
	"encoding/binary"
	"fmt"
)

type int64PlainDecoder struct {
	data []byte

	pos int
}

func (d *int64PlainDecoder) init(data []byte, count int) error {
	d.data = data
	d.pos = 0
	return nil
}

func (d *int64PlainDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []int64:
		return d.decodeInt64(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *int64PlainDecoder) decodeInt64(buf []int64) (n int, err error) {
	i := 0
	for i < len(buf) && d.pos < len(d.data) {
		if d.pos+8 > len(d.data) {
			err = fmt.Errorf("int64/plain: not enough data")
		}
		buf[i] = int64(binary.LittleEndian.Uint64(d.data[d.pos:]))
		d.pos += 8
		i++
	}
	if i == 0 {
		err = fmt.Errorf("int64/plain: no more data")
	}
	return i, err
}

func (d *int64PlainDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]int64, len(buf), len(buf))
	n, err = d.decodeInt64(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
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

func (d *int64DictDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []int64:
		return d.decodeInt64(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *int64DictDecoder) decodeInt64(buf []int64) (n int, err error) {
	keys, err := d.decodeKeys(len(buf))
	if err != nil {
		return 0, err
	}
	for i, k := range keys {
		buf[i] = d.values[k]
	}
	return len(keys), nil
}

func (d *int64DictDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]int64, len(buf), len(buf))
	n, err = d.decodeInt64(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}

type int64DeltaBinaryPackedDecoder struct {
	data []byte

	blockSize     int32
	miniBlocks    int32
	miniBlockSize int32
	numValues     int32

	minDelta        int64
	miniBlockWidths []byte

	pos             int
	i               int
	value           int64
	miniBlock       int
	miniBlockWidth  int
	unpacker        unpack8int64Func
	miniBlockPos    int
	miniBlockValues [8]int64
}

func (d *int64DeltaBinaryPackedDecoder) init(data []byte, count int) error {
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

// page-header := <block size in values> <number of miniblocks in a block> <total value count> <first value>
func (d *int64DeltaBinaryPackedDecoder) readPageHeader() error {
	var n int

	d.blockSize, n = varInt32(d.data[d.pos:])
	if n <= 0 {
		return fmt.Errorf("int64/delta: failed to read block size")
	}
	d.pos += n

	d.miniBlocks, n = varInt32(d.data[d.pos:])
	if n <= 0 {
		return fmt.Errorf("int64/delta: failed to read number of mini blocks")
	}
	// TODO: valdiate d.miniBlocks
	// TODO: do not allocate if not necessary
	d.miniBlockWidths = make([]byte, d.miniBlocks, d.miniBlocks)
	d.pos += n

	d.miniBlockSize = d.blockSize / d.miniBlocks // TODO: rounding

	d.numValues, n = varInt32(d.data[d.pos:])
	if n <= 0 {
		return fmt.Errorf("int64/delta: failed to read total value count")
	}
	d.pos += n

	d.value, n = zigZagVarInt64(d.data[d.pos:])
	if n <= 0 {
		return fmt.Errorf("delta: failed to read first value")
	}
	d.pos += n

	return nil
}

// block := <min delta> <list of bitwidths of miniblocks> <miniblocks>
// min delta : zig-zag var int encoded
// bitWidthsOfMiniBlock : 1 byte little endian
func (d *int64DeltaBinaryPackedDecoder) readBlockHeader() error {
	var n int

	d.minDelta, n = zigZagVarInt64(d.data[d.pos:])
	if n <= 0 {
		return fmt.Errorf("int64/delta: failed to read min delta")
	}
	d.pos += n

	n = copy(d.miniBlockWidths, d.data[d.pos:])
	// TODO: validate <= 32
	if n != len(d.miniBlockWidths) {
		return fmt.Errorf("int64/delta: failed to read all bitwidths of miniblocks")
	}
	d.pos += n

	d.miniBlock = 0

	return nil
}

func (d *int64DeltaBinaryPackedDecoder) decode(slice interface{}) (n int, err error) {
	switch buf := slice.(type) {
	case []int64:
		return d.decodeInt64(buf)
	case []interface{}:
		return d.decodeE(buf)
	default:
		panic("invalid argument")
	}
}

func (d *int64DeltaBinaryPackedDecoder) decodeInt64(buf []int64) (n int, err error) {
	for n < len(buf) && d.i < int(d.numValues) {
		if d.i%8 == 0 {
			if d.i%int(d.miniBlockSize) == 0 {
				if d.miniBlock >= int(d.miniBlocks) {
					err = d.readBlockHeader()
					if err != nil {
						return n, err
					}
				}

				d.miniBlockWidth = int(d.miniBlockWidths[d.miniBlock])
				d.unpacker = unpack8Int64FuncByWidth[d.miniBlockWidth]
				d.miniBlockPos = 0
				d.miniBlock++
			}
			w := int(d.miniBlockWidth)
			if d.pos+w > len(d.data) {
				return n, fmt.Errorf("int64/delta: not enough data")
			}
			d.miniBlockValues = d.unpacker(d.data[d.pos : d.pos+w]) // TODO: validate w
			d.miniBlockPos += w
			d.pos += w
			if d.i+8 >= int(d.numValues) {
				d.pos += int(d.miniBlockSize)/8*w - d.miniBlockPos
			}
		}
		buf[n] = d.value
		d.value += d.miniBlockValues[d.i%8] + d.minDelta
		d.i++
		n++

	}
	if n == 0 {
		return 0, fmt.Errorf("int64/delta: no more data")
	}

	return n, nil
}

func (d *int64DeltaBinaryPackedDecoder) decodeE(buf []interface{}) (n int, err error) {
	b := make([]int64, len(buf), len(buf))
	n, err = d.decodeInt64(b)
	for i := 0; i < n; i++ {
		buf[i] = b[i]
	}
	return n, err
}
