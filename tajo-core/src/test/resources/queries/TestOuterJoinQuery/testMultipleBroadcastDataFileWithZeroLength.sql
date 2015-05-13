select * from customer_large a
 left outer join nation_multifile b on a.c_nationkey = b.n_nationkey
 where b.n_nationkey is null