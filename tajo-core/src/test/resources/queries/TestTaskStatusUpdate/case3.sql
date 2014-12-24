select *
from (
  select a.col1, a.col2, a.key
  from ColumnPartitionedTable a
  join ColumnPartitionedTable b on a.key = b.key
  where
    (a.key = 45.0 or a.key = 38.0)
) test
order by
  col1, col2