create external table table2 (id int, name text, score float, type text) using text
with ('text.delimiter'='ㅎ', 'text.null'='NULL') location ${table.path};

