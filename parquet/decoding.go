package parquet

import "errors"

// TODO: use this in all decoders
var errNED = errors.New("not enough data to decode all values")

type levelsDecoder interface {
	init(data []byte)

	// decodeLevels decodes exactly len(dst) level values into dst slice.
	//
	// It should return errNED if there is not enough data to decode all levels
	// or another error if data is corrupted.
	decodeLevels(dst []uint16) error
}

type valuesDecoder interface {
	init(data []byte) error

	// decode decodes exactly len(dst) values into dst slice.
	//
	// It should return errNED if there is not enough data to decode all values
	// or another error if data is corrupted.
	decode(dst interface{}) error
}

type dictValuesDecoder interface {
	valuesDecoder

	initValues(data []byte, count int) error
}
