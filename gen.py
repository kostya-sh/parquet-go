

print """// Generated Code do not edit.
package main

import (
	"io"
)

type f func([8]int64) []byte

type Codec struct {
	b [32]byte
	encode f
}

// Write makes Codec a writer
func (e *Codec) Write(w io.Writer, values [8]int64) (int,error) {
	return w.Write(e.encode(values))
}
"""
print """
func NewCodec(bitWidth uint) *Codec {
	e := &Codec{}
	switch bitWidth {
"""
for bitWidth in range (1, 33):
	print "\tcase %d:" % bitWidth
	print "\t\te.encode = e.Encode%dRLE" % bitWidth
print"""
	default:
		panic("invalid bitWidth")
	}
	return e
}
"""

for bitWidth in range (1, 33):
	print "func (e *Codec) Encode%dRLE(n [8]int64) []byte { " % bitWidth
	print """
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
			print "\tb[%d] = %s" % (buffer_index , op)
		else:
			print "\tb[%d] |= %s " % (buffer_index, op)

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
				# print "\tb[%d] = byte((n[%d] >> %d) & 0xFF) " % (buffer_index, index, current_byte_index * 8 )
				select_and_shift(buffer_index, current_byte_index, assign=True)
				bit_consumed = bit_pending
				bit_pending = 0
				continue

			# bit_consumed > 0
			if bit_consumed > 0 and bit_consumed + bit_pending > 8:
				# we have to split the current byte in two bytes.
				# first finish the current pending byte
				# the available space is 8-bit_consumed
				#print "\tb[%d] |= byte(((n[%d] & %s) >> %d) & 0xFF) << %d" % (buffer_index, index, bitWidthMask, current_byte_index * 8, bit_consumed)
				select_and_shift(buffer_index, current_byte_index, shift_left=bit_consumed)
				buffer_index+=1

				#print "\tb[%d] |= byte(((n[%d] & %s) >> %d) & 0xFF) >> %d" % (buffer_index, index, bitWidthMask, current_byte_index * 8, (8-bit_consumed))
				select_and_shift(buffer_index, current_byte_index, shift_right=8-bit_consumed)
				current_byte_index+=1
				bit_consumed = min(8,bit_pending) - (8-bit_consumed)
				bit_pending -= min(8,bit_pending)

			elif bit_consumed > 0 and bit_consumed + bit_pending <= 8:
				#print """\tb[%d] |= byte(((n[%d] & %s) >> %d) & 0xFF) << %d""" % (buffer_index, index, bitWidthMask, current_byte_index * 8, bit_consumed)
				select_and_shift(buffer_index, current_byte_index, shift_left=bit_consumed)
				bit_consumed += bit_pending
				bit_pending = 0
				if bit_consumed == 8:
					buffer_index += 1
					bit_consumed = 0
			else:
				raise Exception("impossible %d %d" % (bit_consumed, bit_pending))

			assert bit_pending >= 0, bit_pending

	print "\n\treturn b[:%d]\n}\n" % byteBoundary

for bitWidth in range (1, 32+1):
	print "func (e *Codec) decode%dRLE(b []byte, out []int64) error { " % bitWidth
	print """

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
			print "\tout[%d] = int64(%s)" % (index , op)
		else:
			print "\tout[%d] += int64(%s) " % (index, op)

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

		print "\tout[%d] = int64(%s)" % (index, aggregate(computeByteMask(byteIndexes, bitSlices)))

		continue

		while bit_pending > 0:
			# process byte per byte the current number at index.

			if bit_consumed == 0 and bit_pending >= 8:
				# store the entire byte
				select_and_shift(buffer_index, current_byte_index, assign=assign)
				buffer_index += 1
				bit_consumed = 0
				bit_pending -= 8
				current_byte_index += 1
				continue

			if bit_consumed == 0 and bit_pending < 8:
				# store the value in the remaining of the byte
				# print "\tb[%d] = byte((n[%d] >> %d) & 0xFF) " % (buffer_index, index, current_byte_index * 8 )
				select_and_shift(buffer_index, current_byte_index, assign=assign)
				bit_consumed = bit_pending
				bit_pending = 0
				continue

			# bit_consumed > 0
			if bit_consumed > 0 and bit_consumed + bit_pending > 8:
				# we have to split the current byte in two bytes.
				# first finish the current pending byte
				# the available space is 8-bit_consumed
				#print "\tb[%d] |= byte(((n[%d] & %s) >> %d) & 0xFF) << %d" % (buffer_index, index, bitWidthMask, current_byte_index * 8, bit_consumed)
				select_and_shift(buffer_index, current_byte_index, shift_right=bit_consumed)
				buffer_index+=1

				#print "\tb[%d] |= byte(((n[%d] & %s) >> %d) & 0xFF) >> %d" % (buffer_index, index, bitWidthMask, current_byte_index * 8, (8-bit_consumed))
				select_and_shift(buffer_index, current_byte_index, shift_left=8-bit_consumed)
				current_byte_index+=1
				bit_consumed = min(8,bit_pending) - (8-bit_consumed)
				bit_pending -= min(8,bit_pending)

			elif bit_consumed > 0 and bit_consumed + bit_pending <= 8:
				#print """\tb[%d] |= byte(((n[%d] & %s) >> %d) & 0xFF) << %d""" % (buffer_index, index, bitWidthMask, current_byte_index * 8, bit_consumed)
				select_and_shift(buffer_index, current_byte_index, shift_right=bit_consumed)
				bit_consumed += bit_pending
				bit_pending = 0
				if bit_consumed == 8:
					buffer_index += 1
					bit_consumed = 0
			else:
				raise Exception("impossible %d %d" % (bit_consumed, bit_pending))

			assert bit_pending >= 0, bit_pending

	print "\n\treturn nil\n}\n"
