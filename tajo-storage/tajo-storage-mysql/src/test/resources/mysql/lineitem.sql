create table lineitem (
  l_orderkey    integer not null,
  l_partkey     integer not null,
  l_suppkey     integer not null,
  l_linenumber  integer not null,
  l_quantity    decimal(15,2) not null,
  l_extendedprice  decimal(15,2) not null,
  l_discount    decimal(15,2) not null,
  l_tax         decimal(15,2) not null,
  l_returnflag  char(1) not null,
  l_linestatus  char(1) not null,
  l_shipdate    date not null,
  l_commitdate  date not null,
  l_receiptdate date not null,
  l_shipinstruct char(25) not null,
  l_shipmode     char(10) not null,
  l_comment      varchar(44) not null
);