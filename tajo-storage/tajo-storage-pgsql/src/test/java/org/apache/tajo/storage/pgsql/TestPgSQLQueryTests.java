/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.pgsql;

import org.apache.tajo.QueryTestCaseBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPgSQLQueryTests extends QueryTestCaseBase {
  @SuppressWarnings("unused")
  // This should be invoked for initializing PgSQLTestServer
  private static final String jdbcUrl = PgSQLTestServer.getInstance().getJdbcUrl();

  public TestPgSQLQueryTests() {
    super(PgSQLTestServer.DATABASE_NAME);
  }

  @BeforeClass
  public static void setUp() {
    QueryTestCaseBase.testingCluster.getMaster().refresh();
  }

  @SimpleTest
  @Test
  public void testProjectedColumns() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testFixedLengthFields() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testVariableLengthFields() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testDateTimeTypes() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testTPCH_Q1() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testTPCH_Q2_Part() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testTPCH_Q2_Part_MixedStorage() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testSimpleFilter() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testInPredicateWithNumbers() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testInPredicateWithLiterals() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testCountAsterisk() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testCtasToHdfs() throws Exception {
    try {
      executeString("CREATE DATABASE pgtmp").close();
      executeString("CREATE TABLE pgtmp.offload AS SELECT * FROM LINEITEM").close();

      runSimpleTests();

    } finally {
      executeString("DROP TABLE IF EXISTS pgtmp.offload").close();
      executeString("DROP DATABASE IF EXISTS pgtmp").close();
    }
  }
}
