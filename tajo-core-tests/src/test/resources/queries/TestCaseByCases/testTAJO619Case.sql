select
  count(1)
from
  lineitem as l1 join lineitem as l2 on l1.l_returnflag = l2.l_returnflag;