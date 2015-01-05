select * from region a right outer join customer b
on a.r_regionkey = b.c_custkey
where a.r_name < b.c_name;