select l_orderkey, l_quantity, rank() over (partition by l_orderkey) from lineitem