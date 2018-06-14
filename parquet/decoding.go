package parquet

type levelsDecoder interface {
	init(data []byte, count int)
	decode(levels []int) (n int, err error)
}

type valuesDecoder interface {
	init(data []byte, count int) error
	decode(values interface{}) (n int, err error)
}

type dictValuesDecoder interface {
	valuesDecoder

	initValues(data []byte, count int) error
}
