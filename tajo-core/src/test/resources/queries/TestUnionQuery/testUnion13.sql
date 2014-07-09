select col1, col2, col3
from (
    select
        col1, col2, col3
    from
        (select
            L_RETURNFLAG as col1, L_EXTENDEDPRICE as col2, concat(L_RECEIPTDATE, L_LINESTATUS) as col3
        from
            lineitem
        where col2 > 46796) b
    union all
    select P_TYPE as col1, P_RETAILPRICE * 100 col2, concat('1993', P_NAME) col3 from part
) a
where col3 like '1993%'
