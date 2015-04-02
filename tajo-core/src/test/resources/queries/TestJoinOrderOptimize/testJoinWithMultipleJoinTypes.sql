explain global select * FROM
customer c
right outer join (select n_nationkey from nation) n on n.n_nationkey = c.c_custkey
join region r on r.r_regionkey = c.c_custkey;