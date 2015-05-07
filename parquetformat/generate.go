// package parquetformat provides Thrift types for reading parquet files

package parquetformat

// 1. Get parquet.thrift from apache/parquet-format Github repository

//go:generate wget -O parquet.thrift https://raw.githubusercontent.com/apache/parquet-format/master/src/thrift/parquet.thrift

// 2. Run thrift compiler
//
// TODO: review thrigt_import after
// https://issues.apache.org/jira/browse/THRIFT-3131 is fixed

//go:generate thrift --out .. --gen go:package=$GOPACKAGE,thrift_import=git-wip-us.apache.org/repos/asf/thrift.git/lib/go/thrift parquet.thrift
