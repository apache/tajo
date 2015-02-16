select n_nationkey from nation, region
  where concat(n_name, r_name) like 'UNITED%'