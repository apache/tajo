select
  n1.n_nationkey,
  substr(n1.n_name, 1, 4) name1,
  substr(n2.n_name, 1, 4) name2
from nation n1 join (select * from nation union select * from nation) n2 on substr(n1.n_name, 1, 4) = substr(n2.n_name, 1, 4)
order by n1.n_nationkey;