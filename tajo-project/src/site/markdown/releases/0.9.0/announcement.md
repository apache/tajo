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

# Apache Tajo™ 0.9.0 Release Announcement

The Apache Tajo team is pleased to announce the release of Apache 0.9.0.
Apache Tajo provides low-latency and scalable SQL query 
processing on Hadoop. Due to its advanced design, it is a nice tool at
low-latency ad-hoc queries as well as batch queries.

The release is available for immediate download:

 * http://tajo.apache.org/downloads.html 

This is a major release. Apache Tajo team resolved 330 issues including 
lots of new features, bug fixes, and improvements. Apache Tajo 0.9.0 mainly 
focues on advanced SQL features (window function and multiple distinct 
aggregation), performance improvements (offheap tuple, skewness handling, 
runtime code generation), and scalability on large clusters (improved 
shuffle and fetch).

### Some of highlights

#### More mature SQL features
 * TIMESTAMP, DATE, and TIME Refactoring (TAJO-825)
 * INTERVAL type support (TAJO-761)
 * WINDOW functions and OVER clause support (TAJO-924)
 * ORDER BY and GROUP BY clauses allow column references as well as expressions.
 * Multiple distinct aggregation (TAJO-1010)
 * ORDER BY NULL FIRST support
 * CREATE TABLE LIKE support
 * concat() and concat_ws() function support
 * to_char() for date time format
 * COALESCE() for BOOLEAN, DATE, TIME, and TIMESTAMP

#### Performance Improvements
 * Offheap tuple block and zero-copy tuple (TAJO-907)
 * Offheap sort operator for ORDER BY (TAJO-907)
 * hash shuffle I/O improvement (TAJO-992)
 * Better automatic parallel degree choice for operators involving hash and 
   range shuffles
 * Skewness handling for hash shuffle (TAJO-987)
 * Runtime code generation for evaluating expressions (TAJO-906)
 * Lots of query optimizer improvements

#### Hadoop Integration
 * Hadoop 2.2.0 or higher (to 2.5.1) support
 * Hive Meta Store access support for 0.13.0 and 0.13.1
 * Updated Parquet to 1.5.0. (TAJO-932)

#### Availability, Reliability, and Stability
 * TajoMaster HA (TAJO-704)
 * More stable and error-tolerant fetch (TAJO-789, TAJO-953, TAJO-908, TAJO-949, TAJO-991)

#### Other important improvements
 * MariaDB Catalog Store support (TAJO-847)
 * Linux and HDFS utility support in tajo shell (TAJO-732)


For a complete list of new features and fixed problems, please see the release notes:

 * http://tajo.apache.org/releases/0.9.0/relnotes.html

### Many Thanks to contributors on the 0.9.0 releases

Alvin Henrick, DaeMyung Kang, David Chen, Hyoung Jun Kim, Hyunsik Choi, 
Ilhyun Suh, JaeHwa Jung, Jae Young Lee, Jihun Kang, Jinho Kim, Jihoon Son,
Jongyoung Park, Mai Hai Thanh, Min Zhou, Keuntae Park, Prafulla T, 
Seungun Choe, SeongHwa Ahn, Youngjun Park, and Wan Heo.