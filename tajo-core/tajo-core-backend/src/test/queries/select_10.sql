select B.*
from (
  select A.member_id, A.member_name
  from table1 AS A
) B
