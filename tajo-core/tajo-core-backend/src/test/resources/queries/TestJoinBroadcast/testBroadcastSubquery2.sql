select sum(b.l_quantity)
from (
      select a.l_orderkey, a.l_quantity
        from lineitem_large a
        join part on a.l_partkey = p_partkey) b
join orders c on c.o_orderkey = b.l_orderkey
join (
      select e.l_orderkey, avg(e.l_quantity) avg_quantity
      from (
          select d.l_orderkey, d.l_quantity
            from lineitem_large d
            join part on d.l_partkey = p_partkey
      ) e
      group by e.l_orderkey
) f
on c.o_orderkey = f.l_orderkey
where
  c.o_orderkey > 0 and
  b.l_quantity > f.avg_quantity