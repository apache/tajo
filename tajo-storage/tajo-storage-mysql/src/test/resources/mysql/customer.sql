create table customer (
  c_custkey     integer not null,
  c_name        varchar(25) not null,
  c_address     varchar(40) not null,
  c_nationkey   integer not null,
  c_phone       char(15) not null,
  c_acctbal     decimal(15,2)   not null,
  c_mktsegment  char(10) not null,
  c_comment     varchar(117) not null
);