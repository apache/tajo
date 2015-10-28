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

# Apache Tajo™ 0.11.0 Release Announcement

The Apache Tajo team is pleased to announce the release of Apache Tajo™ 0.11.0!
Apache Tajo™ is a big data warehouse system on various data sources. It provides distributed and scalable SQL analytical processing on Apache Hadoop™.

The release is available for immediate download:

 * http://tajo.apache.org/downloads.html 

This is a major release, and we resolved about 350 issues, including new features, improvements, and bug fixes.

### Some of Highlights
 * Nested record type support (TAJO-1353, TAJO-1359)
 * ORC file support (TAJO-29, TAJO-1464, TAJO-1465)
 * Improved ResultSet fetch performance of JDBC and TajoClient (TAJO-1497)
 * Tablespace support (TAJO-1853, TAJO-1616, TAJO-1868)
 * JDBC storage support and projection/filter push down (TAJO-1730)
 * Multi-query support (TAJO-1397)
 * Python UDF/UDFA support (TAJO-1344, TAJO-1562)

Besides, this release includes improved join optimization, better query response, lots of bug fixes.
 
For a complete list of new features and fixed problems, please see the release notes:

 * http://tajo.apache.org/releases/0.11.0/relnotes.html