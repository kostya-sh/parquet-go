package parquet

import (
	"bytes"
	"testing"
)

func TestVarInt(t *testing.T) {
	tests := []struct {
		value   uint64
		encoded []byte
	}{
		{0x00000000, []byte{0x00}},
		{0x0000007F, []byte{0x7F}},
		{0x00000080, []byte{0x81, 0x00}},
		{0x00002000, []byte{0xC0, 0x00}},
		{0x00003FFF, []byte{0xFF, 0x7F}},
		{0x00004000, []byte{0x81, 0x80, 0x00}},
		{0x001FFFFF, []byte{0xFF, 0xFF, 0x7F}},
		{0x00200000, []byte{0x81, 0x80, 0x80, 0x00}},
		{0x08000000, []byte{0xC0, 0x80, 0x80, 0x00}},
		{0x0FFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0x7F}},
		{0x00FFFFFFFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}}, // maximum write value
	}

	for _, tt := range tests {
		dst, err := writeVarInt(nil, tt.value)
		if err != nil {
			t.Errorf("writeVarInt(0x%X): unexpected error: %s", tt.value, err)
			continue
		}
		if !bytes.Equal(dst, tt.encoded) {
			t.Errorf("writeVarInt(0x%X) = %X, want %X", tt.value, dst, tt.encoded)
		}
		v, i, err := readVarInt(dst, 0)
		if err != nil {
			t.Errorf("readVarInt(%X): unexpected error: %s", dst, err)
			continue
		}
		if v != tt.value || i != len(tt.encoded) {
			t.Errorf("readVarInt(%X) = 0x%X, %d, want 0x%X, %d", dst, v, i, tt.value, len(tt.encoded))
		}
	}
}

func TestVarIntWithIndices(t *testing.T) {
	const want = 0x7F

	dst := append([]byte(nil), 32)
	dst, err := writeVarInt(dst, 0x7F)
	if err != nil {
		t.Fatalf("writeVarInt(0x7F): unexpected error: %s", err)
	}
	if len(dst) != 2 || dst[1] != 0x7F {
		t.Fatalf("writeVarInt(0x7F) = 0x%X", dst)
	}

	for range []int{1, 2, 3} {
		v, i, err := readVarInt(dst, 1)
		if err != nil {
			t.Errorf("readVarInt(0x%X, 1): unexpected error: %s", dst, err)
			continue
		}
		if v != want && i != 2 {
			t.Errorf("readVarInt(0x%X, 1) = %d, %d, want %d, %d", dst, v, i, want, 2)
		}
		dst = append(dst, 1)
	}
}

func TestReadVarIntInvalidInput(t *testing.T) {
	invalidInput := [][]byte{
		// input doesn't end
		{0xFF, 0xFF},

		// value is too large to be represented as uint64
		{0x81, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
	}

	for _, in := range invalidInput {
		_, _, err := readVarInt(in, 0)
		if err == nil {
			t.Errorf("readVarInt(0x%X): expected error", in)
		}
		t.Logf("readVarInt(0x%X): err = %s", in, err)
	}
}

func TestWriteVarIntTooBig(t *testing.T) {
	big := uint64(1 << 56)
	_, err := writeVarInt(nil, big)
	if err == nil {
		t.Errorf("writeVarInt(0x%X): expected error", big)
	}
	t.Logf("writeVarInt(0x%X): err = %s", big, err)
}

func benchmarkWriteVarInt(b *testing.B, val uint64) {
	dst := make([]byte, 10)
	for i := 0; i < b.N; i++ {
		dst, _ = writeVarInt(dst, val)
		dst = dst[:0]
	}
}

func BenchmarkWriteVarInt1(b *testing.B) {
	benchmarkWriteVarInt(b, 0)
}

func BenchmarkWriteVarInt2(b *testing.B) {
	benchmarkWriteVarInt(b, 0x00002000)
}

func BenchmarkWriteVarInt4(b *testing.B) {
	benchmarkWriteVarInt(b, 0x00200000)
}

func BenchmarkWriteVarInt8(b *testing.B) {
	benchmarkWriteVarInt(b, 0x00FFFFFFFFFFFFFF)
}

func benchmarkReadVarInt(b *testing.B, src []byte) {
	for i := 0; i < b.N; i++ {
		_, _, _ = readVarInt(src, 0)
	}
}

func BenchmarkReadVarInt1(b *testing.B) {
	benchmarkReadVarInt(b, []byte{0x00})
}

func BenchmarkReadVarInt2(b *testing.B) {
	benchmarkReadVarInt(b, []byte{0xC0, 0x00})
}

func BenchmarkReadVarInt4(b *testing.B) {
	benchmarkReadVarInt(b, []byte{0x81, 0x80, 0x80, 0x00})
}

func BenchmarkReadVarInt8(b *testing.B) {
	benchmarkReadVarInt(b, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F})
}
