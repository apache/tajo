select avgpy(o_totalprice), countpy(), avg(o_totalprice), count(*) from orders group by o_custkey, o_orderdate