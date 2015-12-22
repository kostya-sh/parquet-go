package parquet

// PLAIN

// RLE + BitPacking Encoder
// Implementation of RLE/Bit-Packing Hybrid encoding

// <encoded-data> part of the following spec:
//
// rle-bit-packed-hybrid: <length> <encoded-data>
// length := length of the <encoded-data> in bytes stored as 4 bytes little endian
// encoded-data := <run>*
// run := <bit-packed-run> | <rle-run>
// bit-packed-run := <bit-packed-header> <bit-packed-values>
// bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
// // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
// bit-pack-count := (number of values in this run) / 8
// bit-packed-values := *see 1 below*
// rle-run := <rle-header> <repeated-value>
// rle-header := varint-encode( (number of times repeated) << 1)
// repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)

// Taken from https://github.com/apache/parquet-cpp/blob/master/src/impala/rle-encoding.h:
// The encoding is:
//    encoded-block := run*
//    run := literal-run | repeated-run
//    literal-run := literal-indicator < literal bytes >
//    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
//    literal-indicator := varint_encode( number_of_groups << 1 | 1)
//    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
//  https://github.com/Parquet/parquet-format/blob/master/Encodings.md
//  https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/util/rle-encoding.h
