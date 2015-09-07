select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost
  from region join nation on n_regionkey = r_regionkey and r_name = 'EUROPE'
  join supplier on s_nationekey = n_nationkey
  join partsupp on s_suppkey = ps_ps_suppkey
  join part on p_partkey = ps_partkey and p_type like '%BRASS' and p_size = 15