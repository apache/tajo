select
    c_custkey,
    c_name,
    n_name
from
    customer_parquet,
    nation_parquet
where
    c_nationkey = n_nationkey
order by
    c_custkey,
    c_name,
    n_name