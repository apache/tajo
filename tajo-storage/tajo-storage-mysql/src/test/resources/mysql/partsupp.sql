create table partsupp (
  ps_partkey     integer not null,
  ps_suppkey     integer not null,
  ps_availqty    integer not null,
  ps_supplycost  decimal(15,2)  not null,
  ps_comment     varchar(199) not null
);