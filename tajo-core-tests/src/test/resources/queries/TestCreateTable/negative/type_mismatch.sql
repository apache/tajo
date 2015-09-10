CREATE TABLE MISMATCH1 (n_name text, n_comment text, n_nationkey int8, n_regionkey int8) AS SELECT * FROM default.nation;
CREATE TABLE MISMATCH2 (n_name text, n_comment text) PARTITION BY COLUMN (n_nationkey int8, n_regionkey int8) AS SELECT * FROM default.nation;
