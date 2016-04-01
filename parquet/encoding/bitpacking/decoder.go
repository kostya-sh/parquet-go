package bitpacking

// type Decoder struct {
// 	r         *bufio.Reader
// 	bitWidth  uint // encoded bits
// 	byteWidth uint // min number of bytes required to encode one <bitWidth> encoded value
// 	value     int64
// 	buff      []byte
// 	bits      uint // number of bits read from the current byte
// 	err       error
// }

// func NewDecoder(r io.Reader, bitWidth uint) *Decoder {
// 	return &Decoder{
// 		r:         bufio.NewReader(r),
// 		bitWidth:  bitWidth,
// 		byteWidth: (bitWidth + uint(7)) / uint(8),
// 	}
// }

// func (d *Decoder) Scan() bool {
// 	var err error

// 	if d.err != nil {
// 		return false
// 	}

// 	if d.buff == nil {
// 		d.buff, err = d.r.Peek(int(d.byteWidth))
// 		if err != nil {
// 			d.setErr(err)
// 			return false
// 		}
// 	}

// 	switch d.byteWidth {
// 	case 1:
// 		// how many bits are left to consume in the current byte
// 		bitsLeftToConsume := 8 - d.bits
// 		missingBits := d.bitWidth - bitsLeftToConsume

// 		// create mask of bitWidth.
// 		// i.e for bitWidth = 3 would be: 0b00000111
// 		mask := byte(0xff >> (8 - d.bitWidth))

// 		value := byte(d.buff[0]>>d.bits) & mask

// 		d.bits += d.bitWidth
// 		d.value = int64(value)

// 		if d.bits >= 8 {
// 			_, err = d.r.Discard(1)

// 			if err != nil {
// 				d.setErr(err)
// 				return false
// 			}

// 			if missingBits > 0 {
// 				// we need to read more to complete the current value
// 				d.bits = missingBits

// 				// read next buffer
// 				d.buff, err = d.r.Peek(int(d.byteWidth))
// 				if err != nil {
// 					d.setErr(err)
// 					return false
// 				}

// 				// mask the relevant bits that we need
// 				// only the first missing bits will be selected
// 				mask = byte(0xff >> (8 - missingBits))

// 				// (d.buff[0] & mask) # select the first missing bits
// 				//  << (d.bitWidth - missingBits)  # shift left to make room
// 				//  | value  # set the previous carry over bits
// 				value = ((d.buff[0] & mask) << (d.bitWidth - missingBits)) | value
// 				d.value = int64(value)
// 			} else {
// 				d.bits = 0
// 				d.buff = nil
// 			}
// 		}
// 		return true
// 	case 2:
// 		d.value = int64(binary.LittleEndian.Uint16(d.buff))
// 	case 3:
// 		b3 := d.buff[0]
// 		b2 := d.buff[1]
// 		b1 := d.buff[2]
// 		d.value = int64(int32(b3) + int32(b2)<<8 + int32(b1)<<16)
// 	case 4:
// 		d.value = int64(binary.LittleEndian.Uint32(d.buff))
// 	default:
// 		panic("unsupported case")
// 	}

// 	return true
// }

// func (d *Decoder) setErr(err error) {
// 	if d.err == nil || d.err == io.EOF {
// 		d.err = err
// 	}
// }

// // Value returns the current value decoded
// func (d *Decoder) Value() int64 {
// 	return d.value
// }

// // Err returns the first non-EOF error that was encountered by the Decoder.
// func (d *Decoder) Err() error {
// 	if d.err == io.EOF {
// 		return nil
// 	}
// 	return d.err
// }
