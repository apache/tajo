select
  name.first_name,
  count(*)
from
  self_desc_table1
group by
  name.first_name