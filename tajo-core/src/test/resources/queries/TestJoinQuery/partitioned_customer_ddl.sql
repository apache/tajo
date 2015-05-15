CREATE TABLE if not exists customer_parts (
  c_custkey    INT4,
  c_name    TEXT,
  c_address    TEXT,
  c_phone    TEXT,
  c_acctbal    FLOAT8,
  c_mktsegment    TEXT,
  c_comment    TEXT
) PARTITION BY COLUMN (c_nationkey INT4) as
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