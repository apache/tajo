select * from ( 
  select a.id, b.c_name, a.code from ( 
    select l_orderkey as id, 'lineitem' as code from lineitem 
    union all 
    select o_orderkey as id, 'order' as code from orders 
   ) a 
   join customer b on a.id = b.c_custkey
) c order by id, code