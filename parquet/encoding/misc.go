package encoding

func trailingZeros(i uint32) uint32 {
	var count uint32

	mask := uint32(1 << 31)
	for mask&i != mask {
		mask >>= 1
		count++
	}
	return count
}

func GetBitWidthFromMaxInt(i uint32) uint {
	return uint(32 - trailingZeros(i))
}

func min(a, b uint) uint {
	if a > b {
		return b
	}
	return a
}
