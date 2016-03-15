select a.c_custkey, b.c_custkey from 
      (select c_custkey, c_nationkey from customer where c_nationkey < 0 
       union all 
       select c_custkey, c_nationkey from customer where c_nationkey > 0 
    ) a 
    left outer join customer b 
    on a.c_custkey = b.c_custkey 