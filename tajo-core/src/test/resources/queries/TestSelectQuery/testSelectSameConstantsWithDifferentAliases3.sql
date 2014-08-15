select
  l_orderkey,
  '20130819' as date1,
  '20130819',
  '20130819',
  '20130819'
from
  lineitem
where
  l_orderkey > -1;