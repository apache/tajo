select
    temp.r_regionkey as r_regionkey
from
    (
    select
        region.r_regionkey as r_regionkey
    from
        region
    ) temp
join
    region b
on
    temp.r_regionkey = b.r_regionkey;