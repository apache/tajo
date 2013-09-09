select A.member_id, A.member_name
from table1 AS A
where A.member_id >= '10000'
and A.member_id <= '20000'
and A.age >= 30
and A.age <= 50