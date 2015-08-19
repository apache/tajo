select n_name from nation
where n_nationkey not in (select r_regionkey from region)
  and n_nationkey not in (select s_nationkey from supplier)