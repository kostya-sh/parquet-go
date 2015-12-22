package rle

import (
	"bufio"
	"encoding/binary"
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

func NewHybridDecoder(r io.Reader, bitWidth int) error {
	rb := bufio.NewReader(r)

	// read count
	length, err := binary.ReadUvarint(rb)
	if err != nil {
		return err
	}

	if length == 0 {
		panic("invalid length")
	}

	isLiteral := length & 1

	if isLiteral == 1 {
		literalCount := (length >> 1) * 8
		log.Println("is literal", literalCount)
	} else {
		repeatCount := length >> 1
		log.Println("is repeat", repeatCount)
	}

	// log.Println(count >> 1)

	// d.count = count >> 1
	// d.hasHeader = true

	// value, err := d.r.ReadByte()
	// if err != nil {
	// 	d.setErr(err)
	// 	return false
	// }

	// var length int32

	// err := binary.Read(rb, binary.LittleEndian, &length)
	// if err != nil {
	// 	return err
	// }

	log.Println("encoded length", length)

	// lr := io.LimitReader(rb, int64(length))

	// d := NewDecoder(lr)

	// for d.Scan() {
	// 	log.Println(d.Value())
	// }

	// if err := d.Err(); err != nil {
	// 	return err
	// }

	// detect encoding
	// dec := bitpacking.NewDecoder(lr, bitWidth)

	// log.Println("values length ", length)

	// for dec.Scan() {
	// 	log.Println(dec.Value())
	// }

	// if err := dec.Err(); err != nil {
	// 	return err
	// }

	return nil
}
