CREATE TABLE customer_broad_parts (
  c_nationkey INT4,
  c_name    TEXT,
  c_address    TEXT,
  c_phone    TEXT,
  c_acctbal    FLOAT8,
  c_mktsegment    TEXT,
  c_comment    TEXT
) PARTITION BY COLUMN (c_custkey    INT4);