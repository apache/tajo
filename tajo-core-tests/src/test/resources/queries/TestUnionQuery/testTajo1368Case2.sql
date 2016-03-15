select * from ( 
  select c_custkey, c_nationkey from ( 
  select c_custkey, c_nationkey from ( 
  select c_custkey, c_nationkey from customer) a 
  union all 
  select c_custkey, c_nationkey from ( 
  select c_custkey, c_nationkey from customer) a 
   ) a 
   ) a 