select
  *
from (
  select
    col1,
    col2,
    null_col,
    key
  from
    testQueryCasesOnColumnPartitionedTable
  where
    (key = 45.0 or key = 38.0) and null_col is null

  union all

  select
    col1,
    col2,
    null_col,
    key
  from
    testQueryCasesOnColumnPartitionedTable
  where
    (key = 45.0 or key = 38.0) and null_col is null

) t1

order by
  col1,
  col2
;