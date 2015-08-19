select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment,
  ps_supplycost,
  r_name,
  p_type,
  p_size
from
  region join nation on n_regionkey = r_regionkey and r_name = 'AMERICA'
  join supplier on s_nationkey = n_nationkey
  join partsupp on s_suppkey = ps_suppkey
  join part on p_partkey = ps_partkey and p_type like '%BRASS' and p_size = 15;