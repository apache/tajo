ALTER TABLE table1 ADD PARTITION (col1 = '2015' , col2 = '01', col3 = '11' )
LOCATION 'hdfs://xxx.com/warehouse/table1/col1=2015/col2=01/col3=11'