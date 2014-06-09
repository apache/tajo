select *
from nation b
join customer a on b.n_nationkey = a.c_nationkey
