select t.n_nationkey, t.name, t.n_regionkey, t.n_comment
from (
  select n_nationkey, n_name as name, n_regionkey, n_comment
  from nation n
  join region r on (n.n_regionkey = r.r_regionkey)
) t
join supplier s on (s.s_nationkey = t.n_nationkey)
where t.name = 'MOROCCO';
