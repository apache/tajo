select n_name from (select * from nation where n_nationkey > 1 and n_nationkey < 10) as T
where n_regionkey in (select r_regionkey from region where r_regionkey > 1 and r_regionkey < 3);