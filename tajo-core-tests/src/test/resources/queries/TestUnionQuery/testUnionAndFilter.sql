SELECT
  c_custkey, ret
FROM (
  SELECT
    c_custkey,
    ROUND(sum(c_acctbal*15000000)/sum(15000000),4) as ret
  FROM
    customer
  GROUP BY
    c_custkey

  UNION

  SELECT
    c_custkey,
    ROUND(sum(c_acctbal*15000000)/sum(15000000),4) AS ret
  FROM
    customer
  GROUP BY
    c_custkey
) a
WHERE
  ret > 0.02;