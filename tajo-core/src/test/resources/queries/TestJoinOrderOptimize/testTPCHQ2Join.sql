explain global select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
order by
  s_acctbal,
  s_name,
  n_name,
  p_partkey;
