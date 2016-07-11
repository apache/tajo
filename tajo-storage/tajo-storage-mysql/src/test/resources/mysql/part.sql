create table part (
  p_partkey     integer not null,
  p_name        varchar(55) not null,
  p_mfgr        char(25) not null,
  p_brand       char(10) not null,
  p_type        varchar(25) not null,
  p_size        integer not null,
  p_container   char(10) not null,
  p_retailprice decimal(15,2) not null,
  p_comment     varchar(23) not null
);