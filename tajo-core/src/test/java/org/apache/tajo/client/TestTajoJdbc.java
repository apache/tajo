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
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.jdbc.JdbcConnection;
import org.apache.tajo.jdbc.TajoDriver;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetSocketAddress;
import java.sql.PreparedStatement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
@NotThreadSafe
public class TestTajoJdbc {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static JdbcConnection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    conf = cluster.getConfiguration();
    ServiceTracker tracker = ServiceTrackerFactory.get(cluster.getConfiguration());
    InetSocketAddress address = tracker.getClientServiceAddress();

    Properties props = new Properties();
    props.setProperty(SessionVars.BLOCK_ON_RESULT.keyname(), "false");

    String rawURI = TajoDriver.TAJO_JDBC_URL_PREFIX + "//" + address.getHostName() + ":" + address.getPort();
    connection = new JdbcConnection(rawURI, props);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  public final void testCancel() throws Exception {
    PreparedStatement statement = connection.prepareStatement("select sleep(1) from lineitem");
    try {
      assertTrue("should have result set", statement.execute());
      TajoResultSetBase result = (TajoResultSetBase) statement.getResultSet();
      Thread.sleep(1000);   // todo query master is not killed properly if it's compiling the query (use 100, if you want see)
      statement.cancel();

      QueryStatus status = connection.getQueryClient().getQueryStatus(result.getQueryId());
      assertEquals(QueryState.QUERY_KILLED, status.getState());
    } finally {
      statement.close();
    }
  }
}
