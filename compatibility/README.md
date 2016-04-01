# Compatibility Test

Read a schema in Avro and output it in parquet format.
Read the generated parquet file using the cpp implementation.

go run ./compatibility/main.go && \
 ~/workspace/tune/parquet-cpp/bin/parquet_reader temp.parquet


go build -o dump ./parqueteur/ && ./dump dump temp.parquet
