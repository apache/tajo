select a.r_regionkey, a.r_name, b.n_name from region a join nation b
on a.r_regionkey = b.n_regionkey
where a.r_name <= b.n_name;