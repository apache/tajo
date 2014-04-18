/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.query;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;

import java.sql.ResultSet;

public class TestJoinOnPartitionedTables extends QueryTestCaseBase {

  public TestJoinOnPartitionedTables() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public void testPartitionTableJoinSmallTable() throws Exception {

    executeDDL("customer_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer.sql");
    res.close();

    res = executeQuery();
    assertResultSet(res);
    res.close();

    res = executeFile("selfJoinOfPartitionedTable.sql");
    assertResultSet(res, "selfJoinOfPartitionedTable.result");
    res.close();

    res = executeFile("testNoProjectionJoinQual.sql");
    assertResultSet(res, "testNoProjectionJoinQual.result");
    res.close();

    res = executeFile("testPartialFilterPushDown.sql");
    assertResultSet(res, "testPartialFilterPushDown.result");
    res.close();
  }
}
