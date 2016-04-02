#
# This scripts generate a bitpackingRLE decoder conforming to the specs in
#
# http://github.com/parquet/parquet-format

fd = open("codec_generate.go", "w")

print >>fd, """// Generated Code do not edit.
package bitpacking

import (
	"io"
	"fmt"
	"bufio"
	"encoding/binary"
)

type format int

const (
	RLE format = iota
	BitPacked
)

type f func([8]int32) []byte

type Encoder struct {
	b [32]byte
	encodeRLE f
	encodeBitPacking f
	format format
}

"""
print >>fd, """
func NewEncoder(bitWidth uint, format format) *Encoder {

	if bitWidth == 0 || bitWidth > 32 {
 		panic("invalid 0 > bitWidth <= 32")
 	}

	e := &Encoder{format:format}
	switch bitWidth {
"""
for bitWidth in range (1, 33):
	print >>fd, "\tcase %d:" % bitWidth
	print >>fd, "\t\te.encodeRLE = e.encode%dRLE" % bitWidth
print >>fd, """
	default:
		panic("invalid bitWidth")
	}
	return e
}

// WriteHeader
func (e *Encoder) WriteHeader(w io.Writer, size uint) error{
	byteWidth := (size + 7)/8
	return binary.Write(w, binary.LittleEndian, (byteWidth << 1))
}

// Write writes in io.Writer all the values
func (e *Encoder) Write(w io.Writer, values []int32) (int,error) {
	total := 0

	var buffer [8]int32
	chunks := (len(values) + 7) / 8

	if e.format == RLE {
		for i:=0; i < chunks; i++ {
			extra := 0
			if (i+1) * 8 > len(values) {
				extra = ((i+1)*8) - len(values)
			}

			for j :=0; j < 8 - extra; j++{
				buffer[j] = values[(i*8)+j]
			}
			for j := extra; j > 0; j-- {
				buffer[j] = 0
			}

			n, err := w.Write(e.encodeRLE(buffer))
			total += n
			if err != nil {
				return total, err
			}
		}

		return total, nil
	}

	return -1, fmt.Errorf("Unsupported")
}
"""

for bitWidth in range (1, 33):
	print >>fd, "func (e *Encoder) encode%dRLE(n [8]int32) []byte { " % bitWidth
	print >>fd, """
	b := e.b
	"""
	# given 8 number how many bytes will be required to encode them
	byteBoundary = ((bitWidth + 7) / 8) * 8

	buffer_index = 0
	bit_consumed = 0

	def select_and_shift(buffer_index, byte_index, shift_right=0, shift_left=0, assign=False):
		bitWidthMask = int( ''.join(['1']*byteBoundary), 2) >> (byteBoundary-bitWidth)
		select_byte = "n[%d]" % index
		if byte_index > 0:
			select_byte = "(%s >> %d)" % (select_byte, byte_index * 8)
		# Mask it
		mask = ("0x%0X" % bitWidthMask) if bitWidthMask < 0xFF else '0xFF'
		select_byte = "(%s & %s)" % (select_byte, mask)

		if shift_right > 0 & shift_left > 0:
			op = "byte((%s >> %d) << %d)" % (select_byte, shift_right, shift_left)
		elif shift_right > 0:
			op = "byte(%s >> %d)" % (select_byte, shift_right)
		elif shift_left > 0:
			op = "byte(%s << %d)" % (select_byte, shift_left)
		else:
			op = "byte(%s)" % (select_byte)

		if assign:
			print >>fd, "\tb[%d] = %s" % (buffer_index , op)
		else:
			print >>fd, "\tb[%d] |= %s " % (buffer_index, op)

	for index in range(0, 8):
		bit_pending = bitWidth
		current_byte_index = 0

		while bit_pending > 0:
			# process byte per byte the current number at index.

			if bit_consumed == 0 and bit_pending >= 8:
				# store the entire byte
				select_and_shift(buffer_index, current_byte_index, assign=True)
				buffer_index += 1
				bit_consumed = 0
				bit_pending -= 8
				current_byte_index += 1
				continue

			if bit_consumed == 0 and bit_pending < 8:
				# store the value in the remaining of the byte
				# print >>fd, "\tb[%d] = byte((n[%d] >> %d) & 0xFF) " % (buffer_index, index, current_byte_index * 8 )
				select_and_shift(buffer_index, current_byte_index, assign=True)
				bit_consumed = bit_pending
				bit_pending = 0
				continue

			# bit_consumed > 0
			if bit_consumed > 0 and bit_consumed + bit_pending > 8:
				# we have to split the current byte in two bytes.
				# first finish the current pending byte
				# the available space is 8-bit_consumed
				#print >>fd, "\tb[%d] |= byte(((n[%d] & %s) >> %d) & 0xFF) << %d" % (buffer_index, index, bitWidthMask, current_byte_index * 8, bit_consumed)
				select_and_shift(buffer_index, current_byte_index, shift_left=bit_consumed)
				buffer_index+=1

				#print >>fd, "\tb[%d] |= byte(((n[%d] & %s) >> %d) & 0xFF) >> %d" % (buffer_index, index, bitWidthMask, current_byte_index * 8, (8-bit_consumed))
				select_and_shift(buffer_index, current_byte_index, shift_right=8-bit_consumed)
				current_byte_index+=1
				bit_consumed = min(8,bit_pending) - (8-bit_consumed)
				bit_pending -= min(8,bit_pending)

			elif bit_consumed > 0 and bit_consumed + bit_pending <= 8:
				#print >>fd, """\tb[%d] |= byte(((n[%d] & %s) >> %d) & 0xFF) << %d""" % (buffer_index, index, bitWidthMask, current_byte_index * 8, bit_consumed)
				select_and_shift(buffer_index, current_byte_index, shift_left=bit_consumed)
				bit_consumed += bit_pending
				bit_pending = 0
				if bit_consumed == 8:
					buffer_index += 1
					bit_consumed = 0
			else:
				raise Exception("impossible %d %d" % (bit_consumed, bit_pending))

			assert bit_pending >= 0, bit_pending

	print >>fd, "\n\treturn b[:%d]\n}\n" % buffer_index

print >>fd, """
type decodef func([]byte, []int32) error

type Decoder struct {
	b [32]byte
	decode decodef
}

func NewDecoder(bitWidth uint) *Decoder {
	d := &Decoder{}

	if bitWidth == 0 || bitWidth > 32 {
 		panic("invalid 0 > bitWidth <= 32")
 	}

	switch bitWidth {
"""
for bitWidth in range (1, 33):
	print >>fd, "\tcase %d:" % bitWidth
	print >>fd, "\t\td.decode = d.decode%dRLE" % bitWidth

print >>fd, """
	default:
		panic("invalid bitWidth")
	}

	return d
}

func (d *Decoder) ReadLength(r io.Reader) (uint,error) {
	// run := <bit-packed-run> | <rle-run>
	header, err := binary.ReadUvarint(bufio.NewReader(r))

	if err == io.EOF {
		return 0, err
	} else if err != nil {
		return 0, err
	}

	if (header & 1) == 1 {
		// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
		// we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
		// bit-pack-count := (number of values in this run) / 8
		literalCount := int32(header>>1)
		return uint(literalCount), nil
	}

	return 0, fmt.Errorf("invalid header: rle header found, expected bitpacking header")
}

func (d *Decoder) Read(r io.Reader, out []int32) error {
	// this assumes len(out) has the exact right
	// amount of data to read
	buffer := make([]int32, 8)
	for i := 0; i < (len(out)+7)/8; i++ {
		n, err := r.Read(d.b[:])
		if err != nil {
			return fmt.Errorf("decodeRLE:%s", err)
		}
		if err := d.decode(d.b[:n], buffer); err != nil{
			return fmt.Errorf("decodeRLE:%s", err)
		}

		extra := 8
		if ((i+1) * 8) > len(out) {
			extra = len(out) - (i * 8)
		}

		for j:=0; j+1 < extra; j++ {
			out[i*8+j] = buffer[j]
		}

	}

	return nil
}
"""

for bitWidth in range (1, 32+1):
	print >>fd, "func (d *Decoder) decode%dRLE(b []byte, out []int32) error { " % bitWidth
	print >>fd, """
	"""
	# given 8 number how many bytes will be required to encode them
	byteBoundary = ((bitWidth + 7) / 8) * 8

	buffer_index = 0
	bit_consumed = 0

	def select_and_shift(buffer_index, byte_index, shift_right=0, shift_left=0, assign=False):
		bitWidthMask = int( ''.join(['1']*byteBoundary), 2) >> (byteBoundary-bitWidth)

		select_byte = "b[%d]" % buffer_index
		# Mask it
		mask = ("0x%0X" % bitWidthMask) if bitWidthMask < 0xFF else '0xFF'
		# select_byte = "(%s & %s)" % (select_byte, mask)

		if shift_right > 0 & shift_left > 0:
			op = "%s >> %d << %d)" % (select_byte, shift_right, shift_left)
		elif shift_right > 0:
			op = "%s >> %d" % (select_byte, shift_right)
		elif shift_left > 0:
			op = "%s << %d" % (select_byte, shift_left)
		else:
			op = "%s" % (select_byte)

		op =  "(%s) & %s" % (op, mask)

		if assign:
			print >>fd, "\tout[%d] = int32(%s)" % (index , op)
		else:
			print >>fd, "\tout[%d] += int32(%s) " % (index, op)

	def selectBytes(byteIndex, mask, shift, rshift=0):
		if mask == "11111111":
			mask = None

		byteSelect = "b[%d]" % byteIndex

		if mask:
			byteSelect = "(%s & 0x%0x)" % (byteSelect, int(mask,2))

		if shift > 0:
			byteSelect = "%s >> %d" % (byteSelect, shift)

		if rshift == 8:
			rshift = 0

		if rshift > 0:
			byteSelect = "(%s) << %d" % (byteSelect, rshift)

		return byteSelect

	def sliceByteMask(bytes, bitSlices):
		splits = len(bytes)
		slices = []
		j = 0
		while splits > 1:
			for i in range(len(bitSlices)):
				if bitSlices[i] == 7:
					slices.append(bitSlices[j:i+1])
					j = i+1
					splits-=1
			slices.append(bitSlices[j:])
			splits -= 1

		if not slices:
			slices = [bitSlices]

		return slices

	def computeByteMask(bytes, bitSlices):
		slices = sliceByteMask(bytes, bitSlices)
		add_ops = []
		for i, b in enumerate(bytes):
			if len(slices[i]) == 1:
				bitToSelect = slices[i][0]
				mask = '1' + '0' * bitToSelect
				shift = bitToSelect
				add_ops.append(selectBytes(b, mask, shift))
				previousShift = 1
			else:
				bitMask = slices[i][-1] - slices[i][0]
				mask = ('1' * (bitMask+1)) + '0' * slices[i][0]
				shift = slices[i][0]
				if i == 0:
					add_ops.append(selectBytes(b, mask, shift))
				else:
					add_ops.append(selectBytes(b, mask, shift, previousShift))
				previousShift = (bitMask+1)

		return add_ops

	def aggregate(operations):
		ops = []
		if bitWidth < 8:
			return ' | '.join(operations)

		for i,op in enumerate(operations):
			if i == 0:
				ops.append("int32(%s)" % op)
			else:
				ops.append("(int32(%s) << %d)" % (op, i * 8))

		return ' + '.join(ops)

	def other(bytes, bitSlices):
		start = None
		masks = []
		maskSize = 0

		for bit in bitSlices:
			if start is None:
				start = bit
				last = bit
				continue

			if bit == last + 1:
				maskSize = (bit-start+1)
				last = bit
				continue
			else:
				# new byte
				mask = '1' * maskSize + '0'*start
				masks.append( ("%s" % mask, " >> %d" % start) )
				maskSize = 0
				start = bit
				last = bit

		if len(masks) != len(bytes):
			lastMask = maskSize
			maskSize = (bit-start+1)
			mask = '1' * maskSize + '0'*start
			masks.append( ("%s" % mask, " >> %d" % start ) )

		return masks

	byteIdx = 0
	for index in range(0, 8):
		bit_pending = 0

		start = (index * bitWidth)
		stop = (index+1) * bitWidth

		byteStart = start / 8
		byteStop = (stop+7) / 8

		bitSlices = [ i%8 for i in range(start, stop)]

		byteIndexes = range(byteStart, byteStop)

		print >>fd, "\tout[%d] = int32(%s)" % (index, aggregate(computeByteMask(byteIndexes, bitSlices)))

	print >>fd, "\n\treturn nil\n}\n"
