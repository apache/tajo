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

# Apache Tajo™ 0.11.1 Release Announcement

The Apache Tajo team is pleased to announce the release of Apache Tajo™ 0.11.1!
Apache Tajo™ is a big data warehouse system on various data sources. It provides distributed and scalable SQL analytical processing on Apache Hadoop™.

The release is available for immediate download:

 * http://tajo.apache.org/downloads.html 

This is a minor release, and we resolved 30 issues, including bug fixes, minor features, and performance improvements.

### Some of Highlights
 * Fix GlobalEngine NPE (TAJO-1753)
 * Fix HBaseStorage NPE (TAJO-1921)
 * Fix memory leak in physical operator (TAJO-1954)
 * Fix Client timezone bug (TAJO-1961)
 * Fix wrong message in TSQL (TAJO-1978)
 * Fix invalid sort order witn NULL FIRST/LAST (TAJO-1972)
 * Fix invalid null handling in Parquet scanner (TAJO-2010)
 * Fix out of memory bug in BSTIndex (TAJO-2000)
 * Reduce memory usage of Hash shuffle (TAJO-1721)
 * Reduce memory usage of QueryMaster during range shuffle (TAJO-1950)
 * Improve join optimization to statistics stored in catalog (TAJO-2007)

For a complete list of new features and fixed problems, please see the release notes:

 * http://tajo.apache.org/releases/0.11.1/relnotes.html
