SELECT *
FROM
(
    SELECT
        l_orderkey,
        l_partkey,
        url
    FROM
        (
          SELECT
            l_orderkey,
            l_partkey,
            CASE
              WHEN
                l_partkey IS NOT NULL THEN ''
              WHEN l_orderkey = 1 THEN '1'
            ELSE
              '2'
            END AS url
          FROM
            lineitem
        ) res1
        JOIN
        (
          SELECT
            *
          FROM
            part
        ) res2
        ON l_partkey = p_partkey
) result




