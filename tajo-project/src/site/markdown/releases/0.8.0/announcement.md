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


## Apache Tajo™ 0.8.0 Release Announcement
 
The Apache Tajo team is pleased to announce the major release of Apache Tajo™ 0.8.0, 
an open source big data warehouse system on Hadoop. Apache Tajo™ provides low-latency and scalable batch SQL queries on large-data sets stored on HDFS and other data sources.

The source and binary release tarballs are available for free download:

 * http://tajo.apache.org/downloads
  
This is the first top-level release. In this release, Apache Tajo team closes 363 issues, including 25 new features, 81 improvement, and 164 bug fixes.

### Some of the highlights:

#### SQL support
 
* Database support (TAJO-353)
* Full/Left/Outer join (TAJO-34, TAJO-312)
* Complex joins including derived subqueries and unions.
* Datetime types and operator support
	* Date type support (TAJO-60, TAJO-438)
	* Time type support (TAJO-61, TAJO-439)
	* Timestamp type support (TAJO-62, TAJO-437)
	* Add datetime operation and functions
		* extract(), to_timestamp(), utc_usec_to()
* SQL standard and PostgreSQL-compatible string function support
	* ascii(), length(), chr(), bit_length(string), hex(), octet_length(), reverse(), right(), left(), md5, repeat, substr(), strpos, locate(), initcap(), lpad(), rpad(), concat(), concat_ws(), ltrim(), rtrim(), btrim(), trim(), and so on.
* SQL standard math function support
	* mod(), div(), degrees(), radians(), cbrt(), abs(), exp(), sqrt(), sin(), sign(), pow(), ceiling(), round(), floor(), ceil(), and so on.
* Other functions
	* sha1(), digest(), find_in_set(), to_bin(), and so on.
* CREATE TABLE AS (CTAS) on partition table (TAJO-460)
* ALTER TABLE RENAME (TAJO-615)
* ALTER TABLE ADD/RENAME COLUMN (TAJO-696, TAJO-697)
* Quoted identifier support (TAJO-644)
* Complex expression support in group-by, order-by, and having clauses
* Distinct aggregation support (TAJO-601)
* Explain statement support (TAJO-122)

#### Performance and Scalability

* More I/O efficient M-way unbalanced external sort executor (TAJO-36, TAJO-584) 
* Reduced intermediate data volume (TAJO-435)
* Star-schema broadcast support (TAJO-725)
* Duplicated expression removal and more efficient projection push down (TAJO-501)
* Reduced memory consumption in expression evaluation (TAJO-539)
* Reduced GC overhead and memory usage (TAJO-522, TAJO-537, TAJO-544, TAJO-548)
* I/O efficient sort-based dynamic partition store method (TAJO-574)

#### Storage support

* Configurable serializer/deserializer of Text file (TAJO-424)
* Parquet file support (TAJO-30, TAJO-714)
* Avro storage support (TAJO-711)
* Amazon S3 support (TAJO-577)

#### Integration with Hadoop ecosystems 

* Hadoop 2.2.0, 2.3.0 or 2.4.0 support
* More improved Hive meta integration (TAJO-289, TAJO-300, TAJO-301)
* Hive-compatible table partition (TAJO-285, TAJO-284, TAJO-338)

#### Client and user interfaces

* More improved WEB UI
* Tajo sql shell (tsql) recap 
* Linux Shell command and HDFS command support in tsql (TAJO-732)
* Tajo JDBC Driver support and its improvements (TAJO-176, TAJO-745)
* Fine-grained query progress indicator (TAJO-589)
* Add killQuery feature (TAJO-305)

### Release Notes:

For a complete list of new features and fixed problems, please see the release notes:

* http://tajo.apache.org/releases/0.8.0/relnotes.html

### Many Thanks to contributors on the 0.8.0 releases

* Alvin Henrick, DaeMyung Kang, David Chen, Hyoung Jun Kim, Hyunsik Choi, Ilhyun Suh, JaeHwa Jung, Jae Young Lee, Jinho Kim, Jihoon Son, Min Zhou, Keuntae Park, Seungun Choe, SeongHwa Ahn, Youngjun Park, Wan Heo.

