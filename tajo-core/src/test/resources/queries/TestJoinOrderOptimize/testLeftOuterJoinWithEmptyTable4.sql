explain global
  select
    max(c_custkey),
    sum(orders.o_orderkey),
    max(orders.o_orderstatus),
    max(orders.o_orderdate)
  from
    customer left outer join orders on c_custkey = o_orderkey
  union
  select
    max(c_custkey),
    sum(empty_orders.o_orderkey),
    max(empty_orders.o_orderstatus),
    max(empty_orders.o_orderdate)
  from
    customer left outer join empty_orders on c_custkey = o_orderkey
;