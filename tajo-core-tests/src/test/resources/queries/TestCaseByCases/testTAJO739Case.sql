select nation.n_nationkey as n_nationkey,
       nation.n_name as n_name
from nation
inner join (select c_nationkey as n_nationkey from customer) a
on nation.n_nationkey = a.n_nationkey