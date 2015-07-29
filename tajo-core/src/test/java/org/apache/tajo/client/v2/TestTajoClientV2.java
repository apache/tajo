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

package org.apache.tajo.client.v2;

import com.facebook.presto.hive.shaded.com.google.common.collect.Lists;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.catalog.exception.DuplicateDatabaseException;
import org.apache.tajo.catalog.exception.UndefinedDatabaseException;
import org.apache.tajo.catalog.exception.UndefinedTableException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class TestTajoClientV2 extends QueryTestCaseBase {
  private static TajoClient clientv2;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = testingCluster.getConfiguration();

    clientv2 = new TajoClient(new ServiceDiscovery() {
      ServiceTracker tracker = ServiceTrackerFactory.get(conf);
      @Override
      public InetSocketAddress clientAddress() {
        return tracker.getClientServiceAddress();
      }
    });
  }

  @AfterClass
  public static void tearDown() throws Exception {
    clientv2.close();
  }

  @Test
  public void testExecuteUpdate() throws TajoException {
    clientv2.executeUpdate("create database tajoclientv2");
    clientv2.selectDB("tajoclientv2");
    clientv2.selectDB("default");
    clientv2.executeUpdate("drop database tajoclientv2");

    try {
      clientv2.selectDB("tajoclientv2");
      fail();
    } catch (UndefinedDatabaseException e) {
    }
  }

  @Test
  public void testExecuteQueryType1() throws TajoException, IOException, SQLException {
    ResultSet res = null;
    try {
      res = clientv2.executeQuery("select * from lineitem");
      assertResultSet(res);
    } finally {
      if (res != null) {
        res.close();
      }
    }
  }

  @Test
  public void testExecuteQueryType2() throws TajoException, IOException, SQLException {
    ResultSet res = null;
    try {
      res = clientv2.executeQuery("select * from lineitem where l_orderkey > 2");
      assertResultSet(res);
    } finally {
      if (res != null) {
        res.close();
      }
    }
  }

  @Test
  public void testExecuteQueryType3() throws TajoException, IOException, SQLException {
    ResultSet res = null;
    try {
      clientv2.executeUpdate("create database client_v2_type3");
      clientv2.selectDB("client_v2_type3");
      clientv2.executeUpdate("create table t1 (c1 int)");
      clientv2.executeUpdate("create table t2 (c2 int)");

      // why we shouldn't use join directly on virtual tables? Currently, join on virtual tables is not supported.
      res = clientv2.executeQuery("select db_id from information_schema.databases where db_name = 'client_v2_type3'");
      assertTrue(res.next());
      int dbId = res.getInt(1);
      res.close();

      res = clientv2.executeQuery(
          "select table_name from information_schema.tables where db_id = " + dbId + " order by table_name");
      assertResultSet(res);
    } finally {
      if (res != null) {
        res.close();
      }

      clientv2.executeUpdate("drop database IF EXISTS client_v2_types3");
    }
  }

  @Test
  public void testExecuteQueryAsync() throws TajoException, IOException, SQLException, ExecutionException,
      InterruptedException {
    QueryFuture future = clientv2.executeQueryAsync("select * from lineitem where l_orderkey > 0");

    ResultSet result = future.get();
    assertResultSet(result);

    assertTrue(future.isDone());
    assertEquals(QueryState.COMPLETED, future.state());
    assertTrue(future.isCompleted());
    assertFalse(future.isFailed());
    assertFalse(future.isKilled());
    assertTrue(1.0f == future.progress());
    assertEquals("default", future.queue());

    assertTrue(future.submitTime() > 0);
    assertTrue(future.startTime() > 0);
    assertTrue(future.finishTime() > 0);

    result.close();
  }

  @Test(timeout = 10 * 1000)
  public void testExecuteQueryAsyncWithListener() throws TajoException, IOException, SQLException, ExecutionException,
      InterruptedException {
    QueryFuture future = clientv2.executeQueryAsync(
        "select l_orderkey, sleep(1) from lineitem where l_orderkey > 3");

    final AtomicBoolean success = new AtomicBoolean(false);
    final List<ResultSet> resultContainer = Lists.newArrayList();

    future.addListener(new FutureListener<QueryFuture>() {
      @Override
      public void processingCompleted(QueryFuture future) {
        try {
          ResultSet result = future.get();
          resultContainer.add(result); // for better error handling, it should be verified outside this future.

          assertTrue(future.isDone());
          assertEquals(QueryState.COMPLETED, future.state());
          assertTrue(future.isCompleted());
          assertFalse(future.isFailed());
          assertFalse(future.isKilled());
          assertTrue(1.0f == future.progress());
          assertEquals("default", future.queue());

          assertTrue(future.submitTime() > 0);
          assertTrue(future.startTime() > 0);
          assertTrue(future.finishTime() > 0);

          success.set(true);

        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
    });

    while(!future.isDone()) {
      Thread.sleep(100);
    }

    assertTrue(success.get());
    assertResultSet(resultContainer.get(0));
    resultContainer.get(0).close();
  }

  @Test(timeout = 10 * 1000)
  public void testQueryFutureKill() throws TajoException, ExecutionException, InterruptedException, SQLException {
    QueryFuture future = clientv2.executeQueryAsync("select sleep(1) from lineitem where l_orderkey > 4");

    assertTrue(future.isOk());
    assertFalse(future.isDone());
    assertFalse(future.isCompleted());
    assertFalse(future.isFailed());
    assertFalse(future.isKilled());

    future.kill();
    while(!future.isDone()) {
      Thread.sleep(100);
    }

    assertTrue(future.isOk());
    assertTrue(future.isDone());
    assertFalse(future.isCompleted());
    assertFalse(future.isFailed());
    assertTrue(future.isKilled());
  }


  @Test(expected = DuplicateDatabaseException.class)
  public void testErrorOnExecuteUpdate() throws TajoException, IOException, SQLException {
    clientv2.executeUpdate("create database default");
  }

  @Test(expected = UndefinedTableException.class)
  public void testErrorOnExecuteQuery() throws TajoException, IOException, SQLException {
    clientv2.executeQuery("select * from unknown_table");
  }

  @Test(expected = UndefinedTableException.class)
  public void testErrorOnExecuteQueryAsync() throws TajoException {
    clientv2.executeQueryAsync("select * from unknown_table");
  }
}
