select * from customer a
 left outer join nation_partitioned b on a.c_nationkey = b.n_nationkey
 where b.n_nationkey is not null