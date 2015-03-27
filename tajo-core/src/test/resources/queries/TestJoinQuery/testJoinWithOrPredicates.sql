select
  n1.n_nationkey,
  n1.n_name,
  n2.n_name
from nation n1, nation n2 where n1.n_name = n2.n_name and (n1.n_nationkey in (1, 2) or n2.n_nationkey in (2))
order by n1.n_nationkey;
