SELECT
  "lineitem".l_orderkey AS l_orderkey,
  "lineitem".l_orderkey AS l_orderkey1,
  COUNT ("lineitem".l_orderkey) AS T57801e5322bc50
FROM
  "lineitem"
GROUP BY
  l_orderkey, l_orderkey1
ORDER BY
  l_orderkey, l_orderkey1;