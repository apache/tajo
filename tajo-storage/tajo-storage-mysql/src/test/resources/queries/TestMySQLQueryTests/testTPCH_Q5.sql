select n_name, revenue::float4 as revenue from (
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey and
	r_name = 'ASIA' and
	o_orderdate >= date '1993-01-01' and o_orderdate < date '1997-01-01'
group by
	n_name
order by
	revenue desc
) w;