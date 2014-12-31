select a.r_regionkey, a.r_name, b.r_name from region a, region b
where a.r_regionkey = b.r_regionkey and a.r_name <= b.r_name;