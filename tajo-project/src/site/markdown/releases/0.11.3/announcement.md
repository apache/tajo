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

# Apache Tajo™ 0.11.3 Release Announcement

The Apache Tajo team is pleased to announce the release of Apache Tajo™ 0.11.3!
Apache Tajo™ is a big data warehouse system on various data sources. It provides distributed and scalable SQL analytical processing on Apache Hadoop™.

The release is available for immediate download:

 * http://tajo.apache.org/downloads.html 

This is a minor release. In this release, we fixed 5 bugs, and temporarily disabled the 'NOT IN' predicate.

### Some of Highlights
 * Fix incorrect DateTime and remove hard coded tests (TAJO-2110)
 * Fix invalid sort result when sort key columns contain non-ascii values. (TAJO-2077)
 * Fix invalid join result when join key columns contain nulls. (TAJO-2135)
 * Fix an empty reason and stacktrace of TajoInternalError (TAJO-2140)
 * Fix race condition in task history writer (TAJO-2143)

For a complete list of new features and fixed problems, please see the release notes:

 * http://tajo.apache.org/releases/0.11.3/relnotes.html
