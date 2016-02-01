# Compatibility Test


Read a schema in Avro and output in parquet.
Read the parquet file using the cpp implementation.

go run ./compatibility/main.go && \
 ~/workspace/tune/parquet-cpp/bin/parquet_reader temp.parquet


go build -o dump ./parqueteur/ && ./dump dump temp.parquet
