select
  sum(distinct l_orderkey),
  l_linenumber,
  count(distinct l_orderkey),
  countpy() as total
from
  (
    select
      *
    from
      lineitem

    union

    select
      *
    from
      lineitem
  ) t1
group by
  l_linenumber;