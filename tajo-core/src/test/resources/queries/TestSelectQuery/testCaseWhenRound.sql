select *
  from (select n_nationkey as key,
               case when n_nationkey > 6 then round((n_nationkey * 100 / 2.123) / (n_regionkey * 50 / 2.123), 2) else 100.0 end as val
          from nation
         where n_regionkey > 0
           and n_nationkey > 0
  ) a
order by a.key