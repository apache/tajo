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

import net.jcip.annotations.NotThreadSafe;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.exception.UndefinedTableException;
import org.apache.tajo.error.Errors;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@NotThreadSafe
public class TestTajoClientFailures {
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
  public final void testCreateDatabase() throws TajoException {
    assertFalse(client.createDatabase("default")); // duplicate database
  }

  @Test
  public final void testDropDatabase() throws TajoException {
    assertFalse(client.dropDatabase("unknown-database")); // unknown database
  }

  @Test
  public final void testDropTable() throws UndefinedTableException {
    assertFalse(client.dropTable("unknown-table")); // unknown table
  }

  @Test
  public void testExecuteSQL() throws SQLException {
    // This is just an error propagation unit test. Specified SQL errors will be addressed in other unit tests.
    ReturnState state = client.executeQuery("select * from unknown_table").getState();
    assertEquals(Errors.ResultCode.UNDEFINED_TABLE, state.getReturnCode());

    state = client.executeQuery("create table default.lineitem (name int);").getState();
    assertEquals(Errors.ResultCode.DUPLICATE_TABLE, state.getReturnCode());
  }
}
