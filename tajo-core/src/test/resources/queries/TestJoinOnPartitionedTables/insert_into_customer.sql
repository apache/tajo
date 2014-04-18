INSERT OVERWRITE INTO customer_parts
  SELECT
    c_custkey,
    c_name,
    c_address,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment,
    c_nationkey
  FROM customer;