select o_orderkey from (select orders.o_orderkey 
    from orders
    join lineitem on orders.o_orderkey = lineitem.l_orderkey
    join customer on orders.o_custkey =  customer.c_custkey
    union all 
    select nation.n_nationkey from nation
) t order by o_orderkey