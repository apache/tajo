(select n1.n_nationkey, n2.n_name from nation n1 join nation n2 on n1.n_nationkey = n2.n_nationkey where n1.n_nationkey = 1);
