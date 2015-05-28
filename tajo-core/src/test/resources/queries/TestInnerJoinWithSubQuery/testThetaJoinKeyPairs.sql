select
  n_nationkey, n_name, n_regionkey, t.cnt
from
  nation n
  join
  (
    select
      r_regionkey, count(*) as cnt
    from
      nation n
      join
      region r
    on (n.n_regionkey = r.r_regionkey)
    group by
      r_regionkey
  ) t
on
  (n.n_regionkey = t.r_regionkey) and n.n_nationkey > t.cnt
order by
  n_nationkey