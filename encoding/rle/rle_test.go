package rle

import (
	"bytes"
	"testing"
)

// Single RLE run: 1-bit per value, 10 x 0
func TestSinlgeRLERun_10x0_1bit(t *testing.T) {
	d, err := NewDecoder(bytes.NewBuffer([]byte{0x14, 0x00}), 1)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		if !d.hasNext() {
			t.Fatalf("want 10 values, got %d", i)
		}
		i32 := d.nextInt32()
		if i32 != 0 {
			t.Errorf("value #%d: got %d, want 0", i+1, i32)
		}
	}
	if d.hasNext() {
		t.Fatalf("Got more than 10 values")
	}
}

// Single RLE run: 20-bits per value, 300x1
func TestSinlgeRLERun_300x1_20bit(t *testing.T) {
	d, err := NewDecoder(bytes.NewBuffer([]byte{0xD8, 0x04, 0x01, 0x00, 0x00}), 20)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < 300; i++ {
		if !d.hasNext() {
			t.Fatalf("want 300 values, got %d", i)
		}
		i32 := d.nextInt32()
		if i32 != 1 {
			t.Errorf("value #%d: got %d, want 1", i+1, i32)
		}
	}
	if d.hasNext() {
		t.Fatalf("Got more than 300 values")
	}
}

// 100 1s followed by 100 0s:
// <varint(100 << 1)> <1, padded to 1 byte>  <varint(100 << 1)> <0, padded to 1 byte>
//  - (total 4 bytes)

// alternating 1s and 0s (200 total):
// 200 ints = 25 groups of 8
// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
// (total 26 bytes, 1 byte overhead)
