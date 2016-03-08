select * from ( 
  select a.id, a.code as code, b.name, b.code as code2 from ( 
    select l_orderkey as id, 'lineitem' as code from lineitem 
    union all 
    select o_orderkey as id, 'order' as code from orders 
  ) a 
  join ( 
    select c_custkey as id, c_name as name, 'customer' as code from customer 
    union all 
    select p_partkey as id, p_name as name, 'part' as code from part 
  ) b on a.id = b.id
) c order by id, code, code2