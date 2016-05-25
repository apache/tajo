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

# Apache Tajo™ 0.11.2 Release Announcement

The Apache Tajo team is pleased to announce the release of Apache Tajo™ 0.11.2!
Apache Tajo™ is a big data warehouse system on various data sources. It provides distributed and scalable SQL analytical processing on Apache Hadoop™.

The release is available for immediate download:

 * http://tajo.apache.org/downloads.html 

This is a minor release, and we resolved 30 issues, including bug fixes, minor features, and performance improvements.

### Some of Highlights
 * Fix incorrect result of join involving an empty table. (TAJO-2077)
 * Fix wrong parsing of date time literal with Timezone. (TAJO-2119)
 * Fix wrong partition pruning with BETWEEN constant folding. (TAJO-2093)
 * Fix ORC table support stored in Hive metastore. (TAJO-2102)
 * Upgrade parquet-mr to 1.8.1. (TAJO-2073)
 * Improve null hanlding in UDFs. (TAJO-2089)

For a complete list of new features and fixed problems, please see the release notes:

 * http://tajo.apache.org/releases/0.11.2/relnotes.html
