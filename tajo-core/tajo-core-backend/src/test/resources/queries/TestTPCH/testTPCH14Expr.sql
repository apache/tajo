select 100 * sum(
  case
    when p_type like 'PROMO%' then l_extendedprice else 0.0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
  lineitem, part
where
  l_partkey = p_partkey;