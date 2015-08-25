select
	s_acctbal,
	s_name,
	p_partkey
from
	part,
	supplier,
	partsupp
where
	p_partkey = ps_partkey and
	s_suppkey = ps_suppkey
order by
  s_acctbal,
  s_name,
  p_partkey;
