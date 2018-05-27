package parquet

type levelsDecoder interface {
	init(data []byte, count int)
	decode(levels []int) (n int, err error)
}

type valuesDecoder interface {
	init(data []byte)
	decode(values interface{}) (n int, err error)
}
