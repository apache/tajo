select
	s.s_acctbal,
	s.s_name,
	n.t_name,
	p.p_partkey,
	p.p_mfgr,
	s.s_address,
	s.s_phone,
	s.s_comment
from (
   select n_name as t_name, n_nationkey as t_nationkey
    , n_regionkey as t_regionkey
    , count(distinct n_comment) as cnt
    , count(distinct n_nationkey / n_regionkey) as diff
   from nation
   where n_nationkey > 10000
   group by n_name, n_nationkey, n_regionkey, n_regionkey
) n
join region r on (n.t_regionkey = r.r_regionkey)
join supplier s on (s.s_nationkey = n.t_nationkey)
join partsupp ps on (s.s_suppkey = ps.ps_suppkey)
join part p on (p.p_partkey = ps.ps_partkey)
where n.t_regionkey = ps.ps_suppkey
and n.cnt > 0
order by
  s.s_acctbal,
  s.s_name,
  n.t_name,
  p.p_partkey;