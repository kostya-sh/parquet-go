package parquet

type Writer interface {
	PutInt32(int32) error
	PutInt64(int64) error
}
