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
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
  @Option(sort = true)
  public void testProjectedColumns() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testFixedLengthFields() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testVariableLengthFields() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testDateTimeTypes() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testTPCH_Q1() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testTPCH_Q2_Part() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testTPCH_Q2_Part_MixedStorage() throws Exception {
    // Manually enable broadcast feature

    try {
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "true");
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_NON_CROSS_JOIN_THRESHOLD.varname,
          "" + (5 * 1024));
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_CROSS_JOIN_THRESHOLD.varname,
          1024 * 1024 + "");

      runSimpleTests();

    } finally {
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname,
          TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.defaultVal);
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_NON_CROSS_JOIN_THRESHOLD.varname,
          TajoConf.ConfVars.$DIST_QUERY_BROADCAST_NON_CROSS_JOIN_THRESHOLD.defaultVal);
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_CROSS_JOIN_THRESHOLD.varname,
          1024 * 1024 + "");
    }
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testTPCH_Q3() throws Exception {
    runSimpleTests();
  }

//  @SimpleTest
//  @Test
  // TODO: enable this test after allowing consecutive cross joins (TAJO-2075)
  @Option(sort = true)
  public void testTPCH_Q5() throws Exception {
    runSimpleTests();
  }

  // Predicates --------------------------------------------------------------

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testSimpleFilter() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testInPredicateWithNumbers() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testInPredicateWithLiterals() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testBetweenNumbers() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testBetweenDates() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testCaseWhenFilter() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testFunctionWithinFilter() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testCountAsterisk() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  @Option(sort = true)
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

  @SimpleTest
  @Test
  @Option(sort = true)
  public void testQueryWithConnProperties() throws Exception {
    Map<String, String> connProperties = new HashMap<>();
    connProperties.put("user", "postgres");
    connProperties.put("password", "");

    Optional<Tablespace> old = Optional.empty();
    try {
      old = PgSQLTestServer.resetAllParamsAndSetConnProperties(connProperties);
      runSimpleTests();
    } finally {
      if (old.isPresent()) {
        TablespaceManager.addTableSpaceForTest(old.get());
      }
    }
  }
}
