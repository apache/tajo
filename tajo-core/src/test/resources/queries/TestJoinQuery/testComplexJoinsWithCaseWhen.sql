select
  r_name,
  case when
    s_name is null then 'N/O'
  else
    s_name
  end as s1
from
  region inner join nation on n_regionkey = r_regionkey
  left outer join supplier on s_nationkey = n_nationkey
order by r_name, s1;