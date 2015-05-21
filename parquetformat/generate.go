// package parquetformat provides Thrift types for reading parquet files

package parquetformat

// 1. Get parquet.thrift from apache/parquet-format Github repository

//go:generate wget -O parquet.thrift https://raw.githubusercontent.com/apache/parquet-format/master/src/thrift/parquet.thrift

// 2. Run thrift compiler
//
//go:generate thrift --out .. --gen go:package=$GOPACKAGE,read_write_private parquet.thrift
