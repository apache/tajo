select *
  from table1
  cross join table2
  join table3 on table1.id = table3.id
  inner join table4 on table1.id = table4.id
  left outer join table5 on table1.id = table5.id
  right outer join table6 on table1.id = table6.id
  full outer join table7 on table1.id = table7.id
  natural join table8
  natural inner join table9
  natural left outer join table10
  natural right outer join table11
  natural full outer join table12