select c_name from customer
where c_nationkey not in (
  select n_nationkey from nation where n_name like 'C%' and n_regionkey not in (
    select r_regionkey from region where r_regionkey > 0 and r_regionkey < 3))