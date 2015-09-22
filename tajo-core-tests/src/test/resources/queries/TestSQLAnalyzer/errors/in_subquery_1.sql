select
  *
from
  lineitem
where l_orderkey in (
  select xxx 1from inner_table
)