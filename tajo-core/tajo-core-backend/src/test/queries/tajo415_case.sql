select
  c_custkey,
  o_orderkey,
  a.cnt

from (

  select
    c_custkey,
    count(*) as cnt

  from
    customer

  group by
    c_custkey

) a left outer join (

  select
    o_orderkey,
    count(*) as cnt

  from
    orders

  where
    o_orderkey is not null

  group by
    o_orderkey

) b on (a.c_custkey = b.o_orderkey);