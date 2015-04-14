explain global select t.n_nationkey, t.n_name, t.n_regionkey, t.n_comment, ps.ps_availqty, s.s_suppkey
from (
  select n_nationkey, n_name, n_regionkey, n_comment
  from nation n
  join region r on (n.n_regionkey = r.r_regionkey)
) t
join supplier s on (s.s_nationkey = t.n_nationkey)
join partsupp ps on (s.s_suppkey = ps.ps_suppkey)
where t.n_name in ('ARGENTINA','ETHIOPIA', 'MOROCCO')
and t.n_nationkey > s.s_suppkey ;
