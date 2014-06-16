INSERT OVERWRITE INTO customer_broad_parts
  SELECT
    c_nationkey,
    c_name,
    c_address,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment,
    c_custkey
  FROM customer;