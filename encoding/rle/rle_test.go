package rle

import (
	"bytes"
	"encoding/binary"
	"testing"
)

var testcases = []struct {
	count int
	value int64
}{
	{1, 1},
	{100, 0},
	{10, 1024},
}

func TestDecoder(t *testing.T) {

	for idx, tc := range testcases {
		b := make([]byte, 9)
		var w bytes.Buffer

		enc := NewHybridBitPackingRLEEncoder(&w)

		renc := Encoder(enc)
		for i := 0; i < 100; i++ {
			renc.Encode(10)
		}

		renc.Flush()

		j := binary.PutUvarint(b, uint64(tc.count<<1))

		b[j] = byte(tc.value)

		d := NewDecoder(bytes.NewBuffer(b[:j+1]))

		for i := 0; i < tc.count; i++ {
			if !d.Scan() {
				t.Fatalf("%d: want %d values, got %d", idx, tc.count, i)
			}

			if v := d.Value(); v != tc.value {
				t.Fatalf("%d: value got %d, want %d", idx, v, tc.value)
			}
		}

		if d.Scan() {
			t.Fatalf("Got more than %d values", tc.count)
		}
	}
}

// // Single RLE run: 1-bit per value, 10 x 0
// func TestSingleRLERun_10x0_1bit(t *testing.T) {
// 	d := NewDecoder(bytes.NewBuffer([]byte{0x14, 0x00}))

// 	for i := 0; i < 10; i++ {
// 		if !d.Scan() {
// 			t.Fatalf("want 10 values, got %d", i)
// 		}

// 		i32 := d.Value()
// 		if i32 != 0 {
// 			t.Errorf("value #%d: got %d, want 0", i+1, i32)
// 		}
// 	}

// 	if d.Scan() {
// 		t.Fatalf("Got more than 10 values")
// 	}
// }

// // Single RLE run: 20-bits per value, 300x1
// func TestSingleRLERun_300x1_20bit(t *testing.T) {
// 	d := NewDecoder(bytes.NewBuffer([]byte{0xD8, 0x04, 0x01, 0x00, 0x00}))

// 	for i := 0; i < 300; i++ {
// 		if !d.Scan() {
// 			t.Fatalf("want 300 values, got %d", i)
// 		}

// 		if i32 := d.Value(); i32 != 1 {
// 			t.Errorf("value #%d: got %d, want 1", i+1, i32)
// 		}
// 	}
// 	if d.Scan() {
// 		t.Fatalf("Got more than 300 values")
// 	}
// }

// // 100 1s followed by 100 0s:
// // <varint(100 << 1)> <1, padded to 1 byte>  <varint(100 << 1)> <0, padded to 1 byte>
// //  - (total 4 bytes)
// func TestSinlgeRLERun_100x1_100x0_1bit(t *testing.T) {
// 	b := make([]byte, 4)
// 	i := binary.PutVarint(b, (100<<1)|0x1)
// 	//	i += binary.PutVarint(b[i:], 0x1)
// 	i += binary.PutVarint(b[i:], (100<<1)|0x0)
// 	//	i += binary.PutVarint(b[i:], 0x0)

// 	t.Logf("%#v", b[:i])

// 	d := NewDecoder(bytes.NewBuffer(b[:i]))

// 	for i := 0; i < 100; i++ {
// 		if !d.Scan() {
// 			t.Fatalf("want 300 values, got %d", i)
// 		}

// 		i32 := d.Value()
// 		if i32 != 1 {
// 			t.Errorf("value #%d: got %d, want 1", i+1, i32)
// 		}
// 	}

// 	for i := 0; i < 100; i++ {
// 		if !d.Scan() {
// 			t.Fatalf("want 300 values, got %d", i)
// 		}

// 		i32 := d.Value()
// 		if i32 != 0 {
// 			t.Errorf("value #%d: got %d, want 0", i+1, i32)
// 		}
// 	}

// 	if d.Scan() {
// 		t.Fatalf("Got more than 300 values")
// 	}
// }

// // alternating 1s and 0s (200 total):
// // 200 ints = 25 groups of 8
// // <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
// // (total 26 bytes, 1 byte overhead)

// func TestRLEncoder(t *testing.T) {
// 	b := make([]byte, 4)
// 	i := binary.PutVarint(b, 100<<1)

// 	bb := bytes.NewBuffer(b[:i])

// 	e := NewEncoder(bb)

// 	if err := e.Encode(1); err != nil {
// 		t.Fatal(err)
// 	}

// 	if err := e.Flush(); err != nil {
// 		t.Fatal(err)
// 	}
// }
