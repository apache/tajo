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

# Apache Tajo™ 0.10.0 Release Announcement

The Apache Tajo team is pleased to announce the release of Apache 0.10.0.
Apache Tajo provides low-latency and scalable SQL analytical 
processing on Hadoop.

The release is available for immediate download:

 * http://tajo.apache.org/downloads.html 

This is a major release. Apache Tajo team resolved about 160 issues including 
lots of new features, bug fixes, and improvements. Apache Tajo 0.10.0 mainly 
focuses on lightweight Tajo client dependencies, HBase storage integration, direct JSON support (flat schema only), and better Amazon S3 support. Also, this release publishes a single JDBC driver jar file separately.

### Some of highlights

#### SQL features
 * Add FIRST_VALUE and LAST_VALUE window functions (TAJO-920)
 * Better TIMEZONE support for TIMESTAMP and TIME types. (TAJO-1185, TAJO-1191, TAJO-1234, and TAJO-1186)
 * Add SET SESSION and RESET statement (TAJO-1238)

#### Performance Improvements
 * Improved performance of delimited text files (TAJO-1100, TAJO-1149, and TAJO-1151)

#### Eco-system Integration
 * Hadoop 2.6.0 support
 * HBase storage integration (TAJO-1118)
 * Better Amazon S3 support ([TAJO-1166, TAJO-1211)

#### Other important improvements 
 * Lightweight Tajo client dependencies - Thin JDBC driver (TAJO-1160, TAJO-1228, and TAJO-1260)
 * Support PostgreSQL CatalogStore (TAJO-233)
 * Support Oracle CatalogStore (TAJO-235)
 * Implement Query history persistency manager (TAJO-1026)
 * Implement Json file scanner (TAJO-1095)

For a complete list of new features and fixed problems, please see the release notes:

 * http://tajo.apache.org/releases/0.10.0/relnotes.html