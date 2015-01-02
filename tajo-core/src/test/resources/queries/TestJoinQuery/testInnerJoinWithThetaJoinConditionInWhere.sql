select a.r_regionkey, a.r_name, b.r_name from region a join region b
on a.r_regionkey = b.r_regionkey
where a.r_name <= b.r_name;