create view revenue[STREAM_ID] (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date '1996-01-01'
		and l_shipdate < date '1996-01-01' + interval '3' month
	group by
		l_suppkey;

select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue[STREAM_ID]
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
		max(total_revenue)
		from
		revenue[STREAM_ID]
	)
order by
	s_suppkey;
	drop view revenue[STREAM_ID];