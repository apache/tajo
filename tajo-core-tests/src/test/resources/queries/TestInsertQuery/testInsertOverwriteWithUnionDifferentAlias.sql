insert overwrite into table1
select l_orderkey as col1, l_partkey as col2, l_quantity as col3 from default.lineitem
union all
select o_orderkey as col4, o_custkey as col5, o_totalprice as col6 from default.orders