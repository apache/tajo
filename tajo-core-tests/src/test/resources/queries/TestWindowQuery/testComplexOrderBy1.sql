select
  l_orderkey,
  row_number() over (order by l_quantity * (1 - l_discount)) row_num
from
  lineitem