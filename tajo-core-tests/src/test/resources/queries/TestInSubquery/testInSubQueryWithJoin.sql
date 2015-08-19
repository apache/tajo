select n_name from nation, supplier
where n_regionkey in (select r_regionkey from region) and n_nationkey = s_nationkey