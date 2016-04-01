

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

	#print bitWidth, (bitWidth +7) / 8, 2 << bitWidth

	#ABC DEF GHI JKL MNO PQR STU VWX ZY1 234 567 89

	#HIDEFABC RMNOJKLG VWXSTUPQ 67234ZY1 00000005


	#print ">"

	# given 8 number how many bytes will be required to encode them
	size_buffer = ( (8 * bitWidth) + 7 ) / 8

	array = ','.join(['0']*size_buffer)

	print "func (e *Codec) Encode%dRLE(n [8]int64) []byte { " % bitWidth
	print """
	b := e.b
	"""

	byteBoundary = ((bitWidth + 7) / 8) * 8

	bitWidthMask = int( ''.join(['1']*byteBoundary), 2) >> (byteBoundary-bitWidth)

	#bitWidthMask = "0x%0X" % bitWidthMask

	buffer_index = 0
	bit_consumed = 0

	def select_and_shift(buffer_index, byte_index, shift_right=0, shift_left=0, assign=False):
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

