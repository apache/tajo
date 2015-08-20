select col1, col2, col3
from (
    select L_RETURNFLAG as col1, L_EXTENDEDPRICE as col2, concat(L_RECEIPTDATE, L_LINESTATUS) as col3 from lineitem
    union all
    select P_TYPE as col1, P_RETAILPRICE col2, P_NAME col3 from part
) a
where col3 like '1993%' and col2 > 46796

