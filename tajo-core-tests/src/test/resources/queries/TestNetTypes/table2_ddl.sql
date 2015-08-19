-- It is used in TestNetTypes

create external table IF NOT EXISTS table2 (id int, name text, score float, type text, addr inet4) using text
with ('text.delimiter'='|', 'text.null'='NULL') location ${table.path};