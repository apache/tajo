-- It is used in TestNetTypes

create external table IF NOT EXISTS table1 (id int, name text, score float, type text, addr inet4) using csv
with ('csvfile.delimiter'='|', 'csvfile.null'='NULL') location ${table.path};