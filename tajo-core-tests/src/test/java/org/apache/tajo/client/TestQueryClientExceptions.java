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

package org.apache.tajo.client;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.annotation.NotThreadSafe;
import org.apache.tajo.error.Errors;
import org.apache.tajo.exception.*;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

@NotThreadSafe
public class TestQueryClientExceptions {
  private static TajoTestingCluster cluster;
  private static TajoClient client;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    client = cluster.newTajoClient();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.close();
  }

  @Test
  public void testExecuteQuery() {
    // This is just an error propagation unit test. Specified SQL errors will be addressed in other unit tests.
    ReturnState state = client.executeQuery("select * from unknown_table").getState();
    assertEquals(Errors.ResultCode.UNDEFINED_TABLE, state.getReturnCode());

    state = client.executeQuery("create table default.lineitem (name int);").getState();
    assertEquals(Errors.ResultCode.DUPLICATE_TABLE, state.getReturnCode());
  }

  @Test(expected = DuplicateTableException.class)
  public void testUpdateQuery() throws TajoException {
    client.updateQuery("create table default.lineitem (name int);");
  }

  @Test(expected = UndefinedTableException.class)
  public void testExecuteQueryAndGetResult() throws TajoException {
    // This is just an error propagation unit test. Specified SQL errors will be addressed in other unit tests.
    client.executeQueryAndGetResult("select * from unknown_table");
  }

  @Test
  public void testCloseQuery() {
    // absent query id
    client.closeQuery(LocalTajoTestingUtility.newQueryId());
    client.closeNonForwardQuery(LocalTajoTestingUtility.newQueryId());
  }

  @Test(expected = UndefinedDatabaseException .class)
  public void testSelectDatabase() throws UndefinedDatabaseException {
    // absent database name
    client.selectDatabase("unknown_db");
  }

  @Test(expected = NoSuchSessionVariableException.class)
  public void testGetSessionVar() throws NoSuchSessionVariableException {
    // absent session variable
    client.getSessionVariable("unknown-var");
  }

  @Test(expected = QueryNotFoundException.class)
  public void testGetQueryResult() throws TajoException {
    // absent query id
    client.getQueryResult(LocalTajoTestingUtility.newQueryId());
  }

  @Test(expected = QueryNotFoundException.class)
  public void testGetResultResponse() throws TajoException {
    // absent query id
    client.getResultResponse(LocalTajoTestingUtility.newQueryId());
  }

  @Test(expected = QueryNotFoundException.class)
  public void testAscynFetchNextQueryResult() throws Throwable {
    Future<TajoMemoryResultSet> future = client.fetchNextQueryResultAsync(LocalTajoTestingUtility.newQueryId(), 100);
    try {
      future.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = QueryNotFoundException.class)
  public void testKillQuery() throws QueryNotFoundException {
    client.killQuery(LocalTajoTestingUtility.newQueryId());
  }

  @Test(expected = QueryNotFoundException.class)
  public void testGetQueryInfo() throws QueryNotFoundException {
    client.getQueryInfo(LocalTajoTestingUtility.newQueryId());
  }

  @Test(expected = QueryNotFoundException.class)
  public void testGetQueryHistory() throws QueryNotFoundException {
    client.getQueryHistory(LocalTajoTestingUtility.newQueryId());
  }
}
