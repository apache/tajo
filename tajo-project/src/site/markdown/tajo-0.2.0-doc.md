<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Apache Tajo 0.2.0-incubating Release Documentation
* Last Updated Date: 2013.11.21

## Table of Contents
* [What is Apache Tajo?](#WhatIsApacheTajo)
* [Tutorial - Getting Started](#GettingStarted)
    * [Prerequisite](#Prerequisite)
    * [Download](#Download) 
          * [Binary Download](#BinaryDownload) 
          * [Source Download](#SourceDownload)           
    * [Installation](#Installation)
        * [Unpack tarball](#UnpackTarball)
        * [Setup a Tajo Cluster](#SetupATajoCluster)
        * [Launch a Tajo Cluster](#LaunchATajoCluster)
    * [First Query Execution](#FirstQueryExecution)
    * [Distributed mode on HDFS cluster](#DistributedMode)
* [Configuration](#Configuration)
    * [Preliminary](#Preliminary)
        * [catalog-site.xml and tajo-site.xml](#catalog-site_and_tajo-site)
    * [TajoMaster Configuration](#TajoMasterConfiguration)
        * [Tajo Rootdir Setting](#TajoRootDir) 
        * [TajoMaster Heap Memory Size](#TajoMasterHeap) 
    * [Tajo Worker Configuration](#TajoWorkerConfiguration)
        * [TajoMaster Heap Memory Size](#TajoMasterHeap) 
        * [Temporary Data Directory](#TemporaryDataDir) 
        * [Maximum number of parallel running tasks for each worker](#MaximumParallelRunningTasks) 
    * [Catalog Configuration](#CatalogConfiguration)
    * [RPC/Http Service Configuration and Default Addresses](#DefaultPorts)
        * [Tajo Master](#TajoMasterDefaultPorts)            
        * [Worker](#TajoWorkerDefaultPorts)
* [Command Line Interface (tsql)](#CommandLineInterface)        
    * [Entering tsql shell](#EnteringTsql)
    * [Meta Commands](#MetaCommands)
    * [Examples](#CLI_Examples)
* [Data Model](#DataModel)        
    * [Data Types](#DataTypes)
        * [Using real number value](#UsingRealNumberValue)
* [SQL Language](#SQLLanguage)        
    * [Data Definition Language (DDL)](#DDL)
        * [CREATE TABLE](#CreateTable)
        * [Compression](#DDLCompression)
    * [Data Manipulation Language (DML)](#DML)
        * [SQL Expressions](#SQLExpressions)
            * [Arithmetic Expressions](#ArithmeticExpressions)
            * [Type Casts](#TypeCasts)
            * [String Expressions](#StringExpressions)
            * [Function Call](#FunctionCall)
        * [SELECT](#Select)
        * [WHERE](#Where)
            * [IN Predicate](#InPredicate)
            * [String Pattern Matching Predicates](#StringPatternMatching) (LIKE, ILIKE, SIMILAR TO, REGULAR EXPRESSIONS)
        * [INSERT (OVERWRITE) INTO](#InsertOverwrite)
    * [Functions](#Functions)
        * [Standard Functions](#StandardFunctions)
        * [String Functions](#StringFunctions)
* [Administration](#Administration)        
    * [Catalog Backup](#CatalogBackup)
        * [SQL dump](#SQLDump)    
        * [Database-level Backup](#DatabaseLevelBackup)    
        
# <a name="WhatIsApacheTajo"></a> What is Apache Tajo?
Tajo is _**a big daga warehouse system on Hadoop**_ that provides low-latency and scalable ad-hoc queries and ETL on large-data sets stored on HDFS and other data sources.

# <a name="GettingStarted"></a>Tutorial - Getting Started

## <a name="Prerequisite"></a>Prerequisite

 * Hadoop 2.0.3-alpha or 2.0.5-alpha
 * Java 1.6 or higher
 * Protocol buffer 2.4.1 
    
## <a name="Download"></a>Download

### <a name="BinaryDownload"></a>Binary Download

Download the source code from http://tajo.apache.org/downloads.html.

### <a name="SourceDownload"></a>Source Download

Download the source code and build Tajo as follows:

```
$ git clone https://github.com/apache/tajo.git tajo
```

## <a name="BuildSourceCode"></a>Build Source Code

You can compile source code and get a binary archive as follows:
 
```
$ cd tajo
$ mvn clean package -DskipTests -Pdist -Dtar
$ ls tajo-dist/target/tajo-x.y.z-SNAPSHOT.tar.gz
```

## <a name="Installation"></a>Installation

### <a name="UnpackTarball"></a>Unpack tarball
You should unpack the tarball (refer to build instruction).

```
$ tar xzvf tajo-0.2.0-SNAPSHOT.tar.gz
```

This will result in the creation of subdirectory named tajo-x.y.z-SNAPSHOT. You MUST copy this directory into the same directory on all cluster nodes.

### <a name="SetupATajoCluster"></a>Setup a Tajo cluster
First of all, you need to add the environment variables to conf/tajo-env.sh.

```sh
# Hadoop home. Required
export HADOOP_HOME= ...

# The java implementation to use.  Required.
export JAVA_HOME= ...
```

## <a name="LaunchATajoCluster"></a>Launch a Tajo cluster
To launch the tajo master, execute start-tajo.sh.

```
$ $TAJO_HOME/bin/start-tajo.sh
```

After then, you can use tajo-cli to access the command line interface of Tajo. If you want to how to use tsql, read Tajo Interactive Shell document.

```
$ $TAJO_HOME/bin/tsql
```

If you type \? on tsql, you can see help documentation. 

## <a name="FirstQueryExecution"></a>First Query Execution
First of all, we need to prepare some data for query execution. For example, you can make a simple text-based table as follows:

```
$ mkdir /home/x/table1
$ cd /home/x/table1
$ cat > data.csv
1|abc|1.1|a
2|def|2.3|b
3|ghi|3.4|c
4|jkl|4.5|d
5|mno|5.6|e
<CTRL + D>
```

This schema of this table is (int, text, float, text).

```
$ $TAJO_HOME/bin/tsql

tajo> create external table table1 (id int, name text, score float, type text) using csv with ('csvfile.delimiter'='|') location 'file:/home/x/table1';
```

In order to load an external table, you need to use 'create external table' statement. In the location clause, you should use the absolute directory path with an appropriate scheme. If the table resides in HDFS, you should use 'hdfs' instead of 'file'.

If you want to know DDL statements in more detail, please see Query Language. 

```
tajo> \d
table1
```

'\d' command shows the list of tables.

```
tajo> \d table1

table name: table1
table path: file:/home/x/table1
store type: CSV
number of rows: 0
volume (bytes): 78 B
schema:
id      INT
name    TEXT
score   FLOAT
type    TEXT
```
'\d [table name]' command shows the description of a given table.

Also, you can execute SQL queries as follows: 

```
tajo> select * from table1 where id > 2;
final state: QUERY_SUCCEEDED, init time: 0.069 sec, response time: 0.397 sec
result: file:/tmp/tajo-hadoop/staging/q_1363768615503_0001_000001/RESULT, 3 rows ( 35B)

id,  name,  score,  type
- - - - - - - - - -  - - -
3,  ghi,  3.4,  c
4,  jkl,  4.5,  d
5,  mno,  5.6,  e

tajo>
```

## <a name="DistributedMode"></a>Distributed mode on HDFS cluster
Add the following configs to tajo-site.xml file.

```
  <property>
    <name>tajo.rootdir</name>
    <value>hdfs://hostname:port/tajo</value>
  </property>

  <property>
    <name>tajo.master.umbilical-rpc.address</name>
    <value>hostname:26001</value>
  </property>

  <property>
    <name>tajo.catalog.client-rpc.address</name>
    <value>hostname:26005</value>
  </property>
```

If you want to know Tajo's configuration in more detail, see Configuration page.

Before launching the tajo, you should create the tajo root dir and set the permission as follows:

```
$ $HADOOP_HOME/bin/hadoop fs -mkdir       /tajo
$ $HADOOP_HOME/bin/hadoop fs -chmod g+w   /tajo
```

Then, execute start-tajo.sh

```
$ $TAJO_HOME/bin/start-tajo.sh
```

Enjoy Apache Tajo!

# <a name="Configuration"></a>Configuration
## <a name="Preliminary"></a>Preliminary
### <a name="catalog-site_and_tajo-site"></a>catalog-site.xml and tajo-site.xml
Tajo's configuration is based on Hadoop's configuration system. Tajo uses two config files:

* catalog-site.xml - configuration for the catalog server.
* tajo-site.xml - configuration for other tajo modules. 

Each config consists of a pair of a name and a value. If you want to set the config name a.b.c with the value 123, add the following element to an appropriate file.

```xml
  <property>
    <name>a.b.c</name>
    <value>123</value>
  </property>
```

Tajo has a variety of internal configs. If you don't set some config explicitly, the default config will be used for for that config. Tajo is designed to use only a few of configs in usual cases. You may not be concerned with the configuration.

In default, there is no tajo-site.xml in ${TAJO}/conf directory. If you set some configs, first copy $TAJO_HOME/conf/tajo-site.xml.templete to tajo-site.xml. Then, add the configs to your tajo-site.

### <a name="tajo-env"></a>tajo-env.sh

tajo-env.sh is a shell script file. The main purpose of this file is to set shell environment variables for TajoMaster and TajoWorker java program. So, you can set some variable as follows:

```
VARIABLE=value
```

If a value is a literal string, type this as follows:

```
VARIABLE='value'
```

### <a name="TajoMasterConfiguration"></a>TajoMaster Configuration

#### <a name="TajoRootDir"></a>Tajo Rootdir Setting

Tajo uses HDFS as a primary storage layer. So, one Tajo cluster instance should have one tajo rootdir. A user is allowed to specific your tajo rootdir as follows:

```
  <property>
    <name>tajo.rootdir</name>
    <value>hdfs://namenode_hostname:port/path</value>
  </property>
```

Tajo rootdir must be a url form like ```scheme://hostname:port/path```. The current implementaion only supports ```hdfs://``` and ```file://``` schemes. The default value is ```file:///tmp/tajo-${user.name}/```.

#### <a name="TajoMasterHeap"></a>TajoMaster Heap Memory Size

The environment variable TAJO_MASTER_HEAPSIZE in conf/tajo-env.sh allow Tajo Master to use the specified heap memory size.

If you want to adjust heap memory size, set TAJO_MASTER_HEAPSIZE variable in conf/tajo-env.sh with a proper size as follows:

```
TAJO_MASTER_HEAPSIZE=2000
```

The default size is 1000 (1GB). 


## <a name="TajoWorkerConfiguration"></a>Tajo Worker Configuration

### <a name="WorkerHeap"></a>Worker Heap Memory Size
The environment variable TAJO_WORKER_HEAPSIZE in conf/tajo-env.sh allow Tajo Worker to use the specified heap memory size.

If you want to adjust heap memory size, set TAJO_WORKER_HEAPSIZE variable in conf/tajo-env.sh with a proper size as follows:

```
TAJO_WORKER_HEAPSIZE=8000
```

The default size is 1000 (1GB).

### <a name="TemporaryDataDir"></a>Temporary Data Directory

TajoWorker stores temporary data on local file system due to out-of-core algorithms. It is possible to specify one or more temporary data directories where temporary data will be stored.

*tajo-site.xml*

```
  <property>
    <name>tajo.worker.tmpdir.locations</name>
    <value>/disk1/tmpdir,/disk2/tmpdir,/disk3/tmpdir</value>
  </property>
```

### <a name="MaximumParallelRunningTasks"></a>Maximum number of parallel running tasks for each worker

Each worker can execute multiple tasks at a time. Tajo allows users to specify the maximum number of parallel running tasks for each worker.

*tajo-site.xml*

```
  <property>
    <name>tajo.worker.parallel-execution.max-num</name>
    <value>12</value>
  </property>
```

## <a name="CatalogConfiguration"></a>Catalog Configuration
If you want to customize the catalog service, copy $TAJO_HOME/conf/catalog-site.xml.templete to catalog-site.xml. Then, add the following configs to catalog-site.xml. Note that the default configs are enough to launch Tajo cluster in most cases.

* tajo.catalog.master.addr - If you want to launch a Tajo cluster in distributed mode, you must specify this address. For more detail information, see [Default Ports](#DefaultPorts).
* tajo.catalog.store.class - If you want to change the persistent storage of the catalog server, specify the class name. Its default value is tajo.catalog.store.DerbyStore. In the current version, Tajo provides three persistent storage classes as follows:
    * tajo.catalog.store.DerbyStore - this storage class uses Apache Derby.
    * tajo.catalog.store.MySQLStore - this storage class uses MySQL.
    * tajo.catalog.store.MemStore - this is the in-memory storage. It is only used in unit tests to shorten the duration of unit tests.

## <a name="DefaultPorts"></a>RPC/Http Service Configuration and Default Addresses

### <a name="TajoMasterDefaultPorts"></a>Tajo Master

| Service Name              | Config Property Name                                         |  Description | default address |
| ------------------------- | -------------------------------------------------------------| ------------ | --------------- |
| Tajo Master Umbilical Rpc | tajo.master.umbilical-rpc.address                            |              | localhost:26001 |
| Tajo Master Client Rpc    | tajo.master.client-rpc.address                               |              | localhost:26002 |
| Tajo Master Info Http     | tajo.master.info-http.address                                |              | 0.0.0.0:26080   |
| Tajo Catalog Client Rpc   | tajo.catalog.client-rpc.address                              |              | localhost:26005 |

### <a name="TajoWorkerDefaultPorts"></a>Worker


| Service Name              | Config Property Name                                         |  Description | default address |
| ------------------------- | -------------------------------------------------------------| ------------ | --------------- |
| Tajo Worker Peer Rpc      | tajo.worker.peer-rpc.address                                 |              | 0.0.0.0:28091   |
| Tajo Worker Client Rpc    | tajo.worker.client-rpc.address                               |              | 0.0.0.0:28092   |
| Tajo Worker Info Http     | tajo.worker.info-http.address                                |              | 0.0.0.0:28080   |


# <a name="CommandLineInterface"></a>Command Line Interface (tsql)

*Synopsis*


```
bin/tsql [options]
```

Options

 * ```-c "quoted sql"``` : Execute quoted sql statements, and then the shell will exist.

 * ```-f filename (--file filename)``` : Use the file named filename as the source of commands instead of interactive shell.

 * ```-h hostname (--host hostname)``` : Specifies the host name of the machine on which the Tajo master is running.

 * ```-p port (--port port)``` : Specifies the TCP port. If it is not set, the port will be 26002 in default. 


## <a name="EnteringTsql"></a>Entering tsql shell

If the hostname and the port num are not given, tsql will try to connect the Tajo master specified in ${TAJO_HOME}/conf/tajo-site.xml.

```
bin/tsql

tajo>
```

If you want to connect a specified TajoMaster, you should use '-h' and (or) 'p' options as follows:

```
bin/tsql -h localhost -p 9004

tajo> 
```

## <a name="MetaCommands"></a>Meta Commands
In tsql, anything command that begins with an unquoted backslash ('\') is a tsql meta-command that is processed by tsql itself.

In the current implementation, there are meta commands as follows:

```
tajo> \?

General
  \copyright  show Apache License 2.0
  \version    show Tajo version
  \?          show help
  \q          quit tsql


Informational
  \d         list tables
  \d  NAME   describe table


Documentations
  tsql guide        http://wiki.apache.org/tajo/tsql
  Query language    http://wiki.apache.org/tajo/QueryLanguage
  Functions         http://wiki.apache.org/tajo/Functions
  Backup & restore  http://wiki.apache.org/tajo/BackupAndRestore
  Configuration     http://wiki.apache.org/tajo/Configuration
```

## <a name="CLI_Examples"></a>Examples

If you want to list all table names, use '\d' meta command as follows:

```
tajo> \d
customer
lineitem
nation
orders
part
partsupp
region
supplier
```

Now look at the table description:

```
tajo> \d orders

table name: orders
table path: hdfs:/xxx/xxx/tpch/orders
store type: CSV
number of rows: 0
volume (bytes): 172.0 MB
schema: 
o_orderkey      INT8
o_custkey       INT8
o_orderstatus   TEXT
o_totalprice    FLOAT8
o_orderdate     TEXT
o_orderpriority TEXT
o_clerk TEXT
o_shippriority  INT4
o_comment       TEXT
```

# <a name="DataModel"></a>Data Model

## <a name="DataTypes"></a>Data Types

| Supported  | SQL Type Name | Alias                    | Size (byte) | Description                                    | Range                                                                    |
| ---------- | ------------- | ------------------------ | ----------- | ---------------------------------------------- | ------------------------------------------------------------------------ |
| O          | boolean       | bool                     | 1           |                                                | true/false                                                               | 
|            | bit           |                          | 1           |                                                | 1/0                                                                      |
|            | varbit        | bit varying              |             |                                                |                                                                          |
| O          | smallint      | tinyint, int2            | 2           | small-range integer value                      | -2^15 (-32,768) to 2^15 (32,767)                                         |
| O          | integer       | int, int4                | 4           | integer value                                  | -2^31 (-2,147,483,648) to 2^31 - 1 (2,147,483,647)                       |
| O          | bigint        | bit varying              | 8           | larger range integer value                     | -2^63 (-9,223,372,036,854,775,808) to 2^63-1 (9,223,372,036,854,775,807) |
| O          | real          | int8                     | 4           | variable-precision, inexact, real number value | -3.4028235E+38 to 3.4028235E+38 (6 decimal digits precision)             |
| O          | float[(n)]    | float4                   | 4 or 8      | variable-precision, inexact, real number value |                                                                          |
| O          | double        | float8, double precision | 8           | variable-precision, inexact, real number value | 1 .7E–308 to 1.7E+308 (15 decimal digits precision)                      |
|            | number        | decimal                  |             |                                                |                                                                          |
|            | char[(n)]     | character                |             |                                                |                                                                          |
|            | varchar[(n)]  | character varying        |             |                                                |                                                                          |
| O          | text          | text                     |             | variable-length unicode text                   |                                                                          |
|            | binary        | binary                   |             |                                                |               |
|            | varbinary[(n)]| binary varying           |             |                                                |               |
| O          | blob          | bytea                    |             | variable-length binary string                  |               |
|            | date          |                          |             |                                                |               |
|            | time          |                          |             |                                                |               |
|            | timetz        | time with time zone      |             |                                                |               |
|            | timestamp     |                          |             |                                                |               |
|            | timestamptz   |                          |             |                                                |               |
| O          | inet4         |                          | 4           | IPv4 address                                   |               |

### <a name="UsingRealNumberValue"></a>Using real number value (real and double)

The real and double data types are mapped to float and double of java primitives respectively. Java primitives float and double follows the IEEE 754 specification. So, these types are correctly matched to SQL standard data types.

 + float[( n )] is mapped to either float or double according to a given length n. If n is specified, it must be bewtween 1 and 53. The default value of n is 53.
  + If 1 <= n <= 24, a value is mapped to float (6 decimal digits precision).
  + If 25 <= n <= 53, a value is mapped to double (15 decimal digits precision). 
 + Do not use approximate real number columns in WHERE clause in order to compare some exact matches, especially the = and <> operators. The > or < comparisons work well. 

	


# <a name="SQLLanguage"></a>The SQL Language

## <a name="DDL"></a>Data Definition Language

### <a name="CreateTable"></a>CREATE TABLE

*Synopsis*


```SQL
CREATE TABLE <table_name> [(<column_name> <data_type>, ... )]
  [using <storage_type> [with (<key> = <value>, ...)]] [AS <select_statement>]

CREATE EXTERNAL TABLE

CREATE EXTERNAL TABLE <table_name> (<column_name> <data_type>, ... )
  using <storage_type> [with (<key> = <value>, ...)] LOCATION '<path>'
```

#### <a name="DDLCompression"></a>Compression

If you want to add an external table that contains compressed data, you should give 'compression.code' parameter to CREATE TABLE statement.

```
create EXTERNAL table lineitem (
  L_ORDERKEY bigint, 
  L_PARTKEY bigint, 
  ...
  L_COMMENT text) 

USING csv WITH ('csvfile.delimiter'='|','compression.codec'='org.apache.hadoop.io.compress.DeflateCodec')
LOCATION 'hdfs://localhost:9010/tajo/warehouse/lineitem_100_snappy';
```

'compression.codec' parameter can have one of the following compression codecs:
 * org.apache.hadoop.io.compress.BZip2Codec
 * org.apache.hadoop.io.compress.DeflateCodec
 * org.apache.hadoop.io.compress.GzipCodec
 * org.apache.hadoop.io.compress.SnappyCodec 

### <a name="DropTable"></a>DROP TABLE

```
DROP TABLE <table_name>
```

## <a name="DML"></a>Data Manipulation Language (DML)

### <a name="SQLExpressions"></a>SQL Expressions

#### <a name="ArithmeticExpressions"></a>Arithmetic Expressions

##### <a name="TypeCasts"></a>Type Casts
A type cast converts a specified-typed data to another-typed data. Tajo has two type cast syntax:

```
CAST ( expression AS type )
expression::type
```

##### <a name="StringExpressions"></a>String Expressions

(TODO)

##### <a name="FunctionCall"></a>Function Call

```
function_name ([expression [, expression ... ]] )
```

### <a name="Select"></a>SELECT

*Synopsis*

```
SELECT [distinct [all]] * | <expression> [[AS] <alias>] [, ...]
  [FROM <table name> [[AS] <table alias name>] [, ...]]
  [WHERE <condition>]
  [GROUP BY <expression> [, ...]]
  [HAVING <condition>]
  [ORDER BY <expression> [ASC|DESC] [NULL FIRST|NULL LAST] [, ...]]
```

### <a name="Where"></a>WHERE

#### <a name="InPredicate"></a>IN Predicate

IN predicate provides row and array comparison.

*Synopsis*

```
column_reference IN (val1, val2, ..., valN)
column_reference NOT IN (val1, val2, ..., valN)
```

Examples are as follows:

```
-- this statement filters lists down all the records where col1 value is 1, 2 or 3:
SELECT col1, col2 FROM table1 WHERE col1 IN (1, 2, 3);

-- this statement filters lists down all the records where col1 value is neither 1, 2 nor 3:
SELECT col1, col2 FROM table1 WHERE col1 NOT IN (1, 2, 3);
```

You can use 'IN clause' on text data domain as follows:

```
SELECT col1, col2 FROM table1 WHERE col2 IN ('tajo', 'hadoop');

SELECT col1, col2 FROM table1 WHERE col2 NOT IN ('tajo', 'hadoop');
```

#### <a name="StringPatternMatching"></a>String Pattern Matching Predicates

##### <a name="LikePredicate"></a>LIKE

LIKE operator returns true or false depending on whether its pattern matches the given string. An underscore (_) in pattern matches any single character. A percent sign (%) matches any sequence of zero or more characters.

*Synopsis*

```
string LIKE pattern
string NOT LIKE pattern
```

##### <a name="ILikePredicate"></a>ILIKE

ILIKE is the same to LIKE, but it is a case insensitive operator. It is not in the SQL standard. We borrow this operator from PostgreSQL.

*Synopsis*

```
string ILIKE pattern
string NOT ILIKE pattern
```

##### <a name="SimilarToPredicate"></a>SIMILAR TO

*Synopsis*

```
string SIMILAR TO pattern
string NOT SIMILAR TO pattern
```

It returns true or false depending on whether its pattern matches the given string. Also like LIKE, 'SIMILAR TO' uses '_' and '%' as metacharacters denoting any single character and any string, respectively.

In addition to these metacharacters borrowed from LIKE, 'SIMILAR TO' supports more powerful pattern-matching metacharacters borrowed from regular expressions:

| metacharacter          | description                                                 |
| ---------------------- | ----------------------------------------------------------- |
| &#124;                 | denotes alternation (either of two alternatives).           |
| *                      | denotes repetition of the previous item zero or more times. |
| +                      | denotes repetition of the previous item one or more times.  |
| ?                      | denotes repetition of the previous item zero or one time.   |
| {m}                    | denotes repetition of the previous item exactly m times.    |
| {m,}                   | denotes repetition of the previous item m or more times.    |
| {m,n}                  | denotes repetition of the previous item at least m and not more than n times. |
| []                     | A bracket expression specifies a character class, just as in POSIX regular expressions. |
| ()                     | Parentheses can be used to group items into a single logical item. |

Note that '.' is not used as a metacharacter in 'SIMILAR TO' operator.

##### <a name="RegularExpressions"></a>Regular expressions

Regular expressions provide a very powerful means for string pattern matching. In the current Tajo, regular expressions are based on Java-style regular expressions instead of POSIX regular expression. The main difference between java-style one and POSIX's one is character class.

*Synopsis*

```
string ~ pattern
string !~ pattern

string ~* pattern
string !~* pattern
```

| operator | Description                                                 |
| ---------| ----------------------------------------------------------- |
| ~        | It returns true if a given regular expression is matched to string. Otherwise, it returns false. |
| !~       | It returns false if a given regular expression is matched to string. Otherwise, it returns true. |
| ~*       | It is the same to '~', but it is case insensitive.  |
| !~*      | It is the same to '!~', but it is case insensitive. |

Here are examples:

```
'abc'   ~   '.*c'               true
'abc'   ~   'c'                 false
'aaabc' ~   '([a-z]){3}bc       true
'abc'   ~*  '.*C'               true
'abc'   !~* 'B.*'               true
```

Regular expressions operator is not in the SQL standard. We borrow this operator from PostgreSQL.

*Synopsis for REGEXP and RLIKE operators*

```
string REGEXP pattern
string NOT REGEXP pattern

string RLIKE pattern
string NOT RLIKE pattern
```

But, they do not support case-insensitive operators.

### <a name="InsertOverwrite"></a>INSERT (OVERWRITE) INTO

INSERT OVERWRITE statement overwrites a table data of an existing table or a data in a given directory. Tajo's INSERT OVERWRITE statement follows 'INSERT INTO SELECT' statement of SQL. The examples are as follows:

```
create table t1 (col1 int8, col2 int4, col3 float8);

-- when a target table schema and output schema are equivalent to each other
INSERT OVERWRITE INTO t1 SELECT l_orderkey, l_partkey, l_quantity FROM lineitem;
-- or
INSERT OVERWRITE INTO t1 SELECT * FROM lineitem;

-- when the output schema are smaller than the target table schema
INSERT OVERWRITE INTO t1 SELECT l_orderkey FROM lineitem;

-- when you want to specify certain target columns
INSERT OVERWRITE INTO t1 (col1, col3) SELECT l_orderkey, l_quantity FROM lineitem;
```

In addition, INSERT OVERWRITE statement overwrites table data as well as a specific directory.

```
INSERT OVERWRITE INTO LOCATION '/dir/subdir' SELECT l_orderkey, l_quantity FROM lineitem;
```




## <a name="Functions"></a>Functions

### <a name="StandardFunctions"></a>Standard Functions

| function definition         | return type       | description         | example                 | result        |
| --------------------------- | ----------------- | ------------------- | ----------------------- | ------------- |
| count(*)                    | int8              |                     |                         |               |
| count(expr)                 | int8              |                     |                         |               |
| avg(expr)                   | depending on expr |                     |                         |               |
| sum(expr)                   | depending on expr |                     |                         |               |
| min(expr)                   | depending on expr |                     |                         |               |
| max(expr)                   | depending on expr |                     |                         |               |


### <a name="StringFunctions"></a>String Operator and Functions


| function definition         | return type   | description         | example                 | result        |
| --------------------------- | ------------- | ------------------- | ----------------------- | ------------- |
| string &#124;&#124; string  | text          | string concatenate  | 'Ta' &#124;&#124; 'jo'  | Tajo          |
| char_length(string text) or character_length(string text) | int | Number of characters in string | char_length('Tajo') | 4 |
| trim([leading &#124; trailing &#124; both] [characters] from string)| text | Remove the characters (a space by default) from the start/end/both ends of the string| trim(both 'x' from 'xTajoxx') | Tajo |
| btrim(string text [, characters text]) | text | Remove the characters (a space by default) from the both ends of the string | trim('xTajoxx', 'x') | Tajo |
| ltrim(string text [, characters text]) | text | Remove the characters (a space by default) from the start ends of the string | ltrim('xxTajo', 'x') | Tajo |
| rtrim(string text [, characters text]) | text | Remove the characters (a space by default) from the end ends of the string | rtrim('Tajoxx', 'x') | Tajo |
| split_part(string text, delimiter text, field int) | text | Split a string on delimiter and return the given field (counting from one) |  split_part('ab_bc_cd','_',2) | bc |
| regexp_replace(string text, pattern text, replacement text) | text | Replace substrings matched to a given regular expression pattern |regexp_replace('abcdef', '(&#710;ab&#124;ef$)', '--') | --cd-- |
| upper(string text) | text | makes an input text to be upper case | upper('tajo') | TAJO | 
| lower(string text) | text | makes an input text to be lower case | lower('TAJO') | tajo |

# <a name="Administration"></a>Administration

## <a name="CatalogBackup"></a>Catalog Backup and Restore

Now, Tajo supports a two backup methods for 

* SQL dump
* Database-level backup 

### <a name="SQLDump"></a>SQL dump

SQL dump is an easy and strong way. If you use this approach, you don't need to concern database-level compatibilities. If you want to backup your catalog, just use bin/tajo_dump command. The basic usage of this command is:

```
$ tajo_dump table_name > outfile
```

For example, if you want to backup a table customer, you should type a command as follows:

```
$ bin/tajo_dump customer > table_backup.sql
$
$ cat table_backup.sql
-- Tajo database dump
-- Dump date: 10/04/2013 16:28:03
--

--
-- Name: customer; Type: TABLE; Storage: CSV
-- Path: file:/home/hyunsik/tpch/customer
--
CREATE EXTERNAL TABLE customer (c_custkey INT8, c_name TEXT, c_address TEXT, c_nationkey INT8, c_phone TEXT, c_acctbal FLOAT8, c_mktsegment TEXT, c_comment TEXT) USING CSV LOCATION 'file:/home/hyunsik/tpch/customer';
```

If you want to restore the catalog from the SQL dump file, please type the below command:

```
$ bin/tsql -f table_backup.sql
```

If you use an option '-a', tajo_dump will dump all table DDLs.

```
$ bin/tajo_dump -a > all_backup.sql
```

### <a name="DatabaseLevelBackup"></a>Database-level backup

(TODO)

