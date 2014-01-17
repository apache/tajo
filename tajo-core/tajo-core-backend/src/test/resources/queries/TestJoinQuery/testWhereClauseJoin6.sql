select
	s_acctbal,
	s_name,
	p_partkey,
	n_name,
from
	part,
	supplier,
	partsupp,
	nation
where
	p_partkey = ps_partkey and
	s_suppkey = ps_suppkey and
	s_nationkey = n_nationkey
order by
  s_acctbal,
  s_name,
  p_partkey,
  n_name;
