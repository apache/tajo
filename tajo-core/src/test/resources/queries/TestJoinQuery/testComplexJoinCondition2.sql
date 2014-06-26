select
  n1.n_nationkey,
  n1.n_name,
  upper(n2.n_name) as name
from nation n1 join nation n2 on n1.n_name = upper(n2.n_name)
order by n1.n_nationkey;