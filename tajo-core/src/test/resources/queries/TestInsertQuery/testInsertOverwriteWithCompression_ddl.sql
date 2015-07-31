create table
  testInsertOverwriteWithCompression (col1 int4, col2 int4, col3 float8)
USING text
WITH ('text.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec');