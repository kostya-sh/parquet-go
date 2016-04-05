package rle

// type RLEWriter interface {
// 	Write(count uint64, value int64) error
// }

// // Decoder is a simple RLE decoder
// type Decoder struct {
// 	r         *bufio.Reader
// 	count     uint64
// 	value     int64
// 	err       error
// 	hasHeader bool
// }

// func NewDecoder(r io.Reader) *Decoder {
// 	return &Decoder{r: bufio.NewReader(r)}
// }

// func (d *Decoder) Scan() bool {
// 	if d.err != nil {
// 		return false
// 	}

// 	if !d.hasHeader {
// 		// read count
// 		count, err := binary.ReadUvarint(d.r)
// 		if err != nil {
// 			d.setErr(err)
// 			return false
// 		}

// 		if count == 0 {
// 			panic("invalid count")
// 		}

// 		d.count = count >> 1
// 		d.hasHeader = true

// 		value, err := d.r.ReadByte()
// 		if err != nil {
// 			d.setErr(err)
// 			return false
// 		}

// 		d.value = int64(value)

// 	}

// 	if d.count == 0 {
// 		return false
// 	}

// 	d.count--
// 	return true
// }

// func (d *Decoder) Value() int64 {
// 	return d.value
// }

// func (d *Decoder) setErr(err error) {
// 	if d.err == nil || d.err == io.EOF {
// 		d.err = err
// 	}
// }

// func (d *Decoder) Err() error {
// 	if d.err == io.EOF {
// 		return nil
// 	}

// 	return d.err
// }

// // func GetInt32(r io.Reader, bitWidth uint) int32 {
// // 	br := newBitReader(r)
// // 	return br.ReadBits(uint)
// // 	length, err := binary.ReadUvarint(rb)
// // 	if err != nil {
// // 		return -1
// // 	}

// // 	log.Printf("repetition encoded length: %d", length)
// // 	for i := uint64(0); i < length; i++ {
// // 		if _, err := rb.ReadByte(); err != nil {
// // 			return nil, nil, err
// // 		}
// // 	}

// // }

// // An Encoder serializes data in the RLE format.
// type Encoder struct {
// 	w     RLEWriter // where to send the data
// 	value int64     // last seen values
// 	count uint64    // how many times we have seen the value
// }

// func NewEncoder(w RLEWriter) *Encoder {
// 	return &Encoder{w: w}
// }

// func (e *Encoder) Encode(value int64) error {
// 	if value != e.value {
// 		if err := e.Flush(); err != nil {
// 			return err
// 		}
// 		e.value = value
// 		e.count = 1
// 	} else {
// 		e.count++
// 	}

// 	return nil
// }

// // Flush writes the current running value in the underlying writer
// func (e *Encoder) Flush() (err error) {
// 	return e.w.Write(e.count, e.value)
// }
