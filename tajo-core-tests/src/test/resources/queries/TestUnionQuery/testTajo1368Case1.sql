select * from 
       (select c_custkey, c_nationkey from customer where c_nationkey < 0 
       union all 
       select c_custkey, c_nationkey from customer where c_nationkey > 0 
    ) a 
    union all 
    select * from 
       (select c_custkey, c_nationkey from customer where c_nationkey < 0 
       union all 
       select c_custkey, c_nationkey from customer where c_nationkey > 0 
    ) b
