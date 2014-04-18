-- Outer Join's Left Table
-- It is used in TestJoin::testOuterJoinAndCaseWhen

create external table if not exists table1 (id int, name text, score float, type text) using csv
with ('csvfile.delimiter'='|', 'csvfile.null'='NULL') location ${table.path};

