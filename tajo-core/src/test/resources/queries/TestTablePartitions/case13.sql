select
    key_alias as key, cnt
from (
    select
        key as key_alias,
        count(*) cnt
    from
        testQueryCasesOnColumnPartitionedTable
    group by key_alias
    order by key_alias desc
) a