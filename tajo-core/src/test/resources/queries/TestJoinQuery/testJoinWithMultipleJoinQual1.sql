select
	s.s_acctbal,
	s.s_name,
	n.n_name,
	p.p_partkey,
	p.p_mfgr,
	s.s_address,
	s.s_phone,
	s.s_comment
from nation n
join region r on (n.n_regionkey = r.r_regionkey)
join supplier s on (s.s_nationkey = n.n_nationkey)
join partsupp ps on (s.s_suppkey = ps.ps_suppkey)
join part p on (p.p_partkey = ps.ps_partkey)
where n.n_regionkey = ps.ps_suppkey
order by
  s.s_acctbal,
  s.s_name,
  n.n_name,
  p.p_partkey;