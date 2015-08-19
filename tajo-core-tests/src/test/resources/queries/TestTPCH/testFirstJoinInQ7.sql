select
  n1.n_name as supp_nation, n2.n_name as cust_nation, n1.n_nationkey as s_nationkey, n2.n_nationkey as c_nationkey
from
  nation n1 join nation n2
  on
    n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY'
UNION ALL
select
  n1.n_name as supp_nation, n2.n_name as cust_nation, n1.n_nationkey as s_nationkey, n2.n_nationkey as c_nationkey
from
  nation n1 join nation n2
  on
    n2.n_name = 'FRANCE' and n1.n_name = 'GERMANY'