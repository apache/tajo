create table
  testInsertOverwriteWithCompression (col1 int4, col2 int4, col3 float8)
USING csv
WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec');