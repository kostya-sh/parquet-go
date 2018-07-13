package parquet

import "errors"

// TODO: use this in all decoders
var errNED = errors.New("not enough data to decode all values")

type levelsDecoder interface {
	init(data []byte)

	// decodeLevels decodes exactly len(dst) level values into dst
	// slice. len(dst) should be > 0.
	//
	// It should return errNED if there is not enough data to decode all levels
	// or another error if data is corrupted.
	//
	// TODO: implementations should panic when len(dst) = 0
	decodeLevels(dst []uint16) error
}

type valuesDecoder interface {
	init(data []byte) error

	// decode decodes exactly len(dst) values into dst slice. len(dst) should be
	// > 0.
	//
	// It should return errNED if there is not enough data to decode all values
	// or another error if data is corrupted.
	//
	// TODO: implementations should panic when len(dst) = 0
	decode(dst interface{}) error
}

type dictValuesDecoder interface {
	valuesDecoder

	initValues(data []byte, count int) error
}
