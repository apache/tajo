SELECT l_orderkey FROM (

-- actual test query
  SELECT
    T1.l_orderkey
  FROM
    LINEITEM
  INNER JOIN (
    SELECT
      T1.l_orderkey
    FROM (
      SELECT
        LINEITEM.l_orderkey AS l_orderkey
      FROM
        LINEITEM
    ) T1
  ) T1 ON LINEITEM.l_orderkey=T1.l_orderkey

) A ORDER BY l_orderkey; -- for determinant query result