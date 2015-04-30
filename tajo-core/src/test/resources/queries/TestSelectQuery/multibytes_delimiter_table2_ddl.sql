create external table table2 (id int, name text, score float, type text) using csv
with ('csvfile.delimiter'='ㅎ', 'csvfile.null'='NULL') location ${table.path};

