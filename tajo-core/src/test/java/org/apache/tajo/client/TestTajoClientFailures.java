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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.error.Errors;
import org.apache.tajo.exception.ErrorUtil;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.QueryHistoryProto;
import org.apache.tajo.ipc.ClientProtos.QueryInfoProto;
import org.apache.tajo.ipc.ClientProtos.StageHistoryProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
@NotThreadSafe
public class TestTajoClientFailures {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static TajoClient client;
  private static Path testDir;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    conf = cluster.getConfiguration();
    client = cluster.newTajoClient();
    testDir = CommonTestingUtil.getTestDir();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.close();
  }

  @Test
  public final void testCreateDatabase() throws SQLException {
    assertFalse(client.createDatabase("12345")); // invalid name
    assertFalse(client.createDatabase("default")); // duplicate database
  }

  @Test
  public final void testDropDatabase() throws SQLException {
    assertFalse(client.dropDatabase("unknown-database")); // duplicate database
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
