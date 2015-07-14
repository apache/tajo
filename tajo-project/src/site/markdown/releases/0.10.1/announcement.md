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

# Apache Tajo™ 0.10.1 Release Announcement

The Apache Tajo team is pleased to announce the release of Apache Tajo™ 0.10.1.
Apache Tajo™ is a big data warehouse system on various data sources. It provides distributed and scalable SQL analytical processing on Apache Hadoop™.

The release is available for immediate download:

 * http://tajo.apache.org/downloads.html 

This is a minor release for bug fixes. For this release, Apache Tajo team resolved about 51 issues including bug fixes, improvements, and few new features.

### Some of Highlights
 * Support multi-bytes delimiter for CSV/Text file ([TAJO-1374] (https://issues.apache.org/jira/browse/TAJO-1374), [TAJO-1381] (https://issues.apache.org/jira/browse/TAJO-1381))
 * JDBC program is stuck after closing ([TAJO-1619] (https://issues.apache.org/jira/browse/TAJO-1619))
 * INSERT INTO with wrong target columns causes NPE. ([TAJO-1623] (https://issues.apache.org/jira/browse/TAJO-1623))
 * Add TajoStatement::setMaxRows method support ([TAJO-1400] (https://issues.apache.org/jira/browse/TAJO-1400))
 * Fix NPE on natural join ([TAJO-1574] (https://issues.apache.org/jira/browse/TAJO-1574))
 * Implement json\_extract\_path\_text(string, string) function ([TAJO-1529] (https://issues.apache.org/jira/browse/TAJO-1529))
 * CURRENT_DATE generates parsing errors sometimes. ([TAJO-1386] (https://issues.apache.org/jira/browse/TAJO-1386))
 * Simple query doesn't work in Web UI. ([TAJO-1147] (https://issues.apache.org/jira/browse/TAJO-1147))
 
For a complete list of new features and fixed problems, please see the release notes:

 * http://tajo.apache.org/releases/0.10.1/relnotes.html