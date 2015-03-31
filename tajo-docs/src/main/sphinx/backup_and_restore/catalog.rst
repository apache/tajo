***************************
Backup and Restore Catalog
***************************

Now, Tajo supports a two backup methods for 

* SQL dump
* Database-level backup 

==========
SQL dump 
==========

SQL dump is an easy and strong way. If you use this approach, you don't need to concern database-level compatibilities. If you want to backup your catalog, just use bin/tajo-dump command. The basic usage of this command is: ::

  $ tajo-dump table_name > outfile

For example, if you want to backup a table customer, you should type a command as follows: ::

  $ bin/tajo-dump customer > table_backup.sql
  $
  $ cat table_backup.sql
  -- Tajo database dump
  -- Dump date: 10/04/2013 16:28:03
  --

  --
  -- Name: customer; Type: TABLE; Storage: CSV
  -- Path: file:/home/hyunsik/tpch/customer
  --
  CREATE EXTERNAL TABLE customer (c_custkey INT8, c_name TEXT, c_address TEXT, c_nationkey INT8, c_phone TEXT, c_acctbal FLOAT8, c_mktsegment TEXT, c_comment TEXT) USING TEXT LOCATION 'file:/home/hyunsik/tpch/customer';
  

If you want to restore the catalog from the SQL dump file, please type the below command: ::

  $ bin/tsql -f table_backup.sql


If you use an option '-a', tajo-dump will dump all table DDLs. ::

  $ bin/tajo-dump -a > all_backup.sql

=======================
Database-level backup
=======================

.. todo::

