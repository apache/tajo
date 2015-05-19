-- Sort Table
-- It is used in TestSortQuery::testSortWithDate

create external table testSortWithDate (
  col1 timestamp,
	col2 date,
	col3 time
) using csv
with ('csvfile.delimiter'='|', 'csvfile.null'='NULL')
location ${table.path};