select
  a.id,
  a.name,
  b.id as id2,
  b.name as name2,
  case when b.name is null then '9991231' else b.name end as c1,
  case when c.name is null then '9991231' else c.name end as c2
from
  table1 a left outer join table2 b on a.id = b.id left outer join table1 c on b.id = c.id
order by
  a.id,
  a.name;