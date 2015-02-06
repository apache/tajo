create external table tabledelimiter (id int, name text, score float) using csv
with ('csvfile.delimiter'=';') location ${table.path};