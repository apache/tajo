/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.master;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import tajo.QueryId;
import tajo.TajoProtos.QueryState;
import tajo.TajoProtos.TaskAttemptState;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.client.TajoClient;
import tajo.conf.TajoConf;
import tajo.datum.DatumFactory;
import tajo.storage.Appender;
import tajo.storage.StorageManager;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestFaultTolerance {

  public static final String TEST_DIRECTORY = "/tajo";

  private static MockupCluster cluster;
  private static TajoClient client;
  private static Configuration conf;
  private static Schema schema;
  private static TableMeta testMeta;
  private static TableDesc testDesc;
  private static String query;

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new MockupCluster(3, 0, 2);
    conf = cluster.getConf();
    cluster.start();
    client = new TajoClient((TajoConf) conf);

    query = "select * from test";
    schema = new Schema();
    schema.addColumn("deptname", DataType.STRING);
    schema.addColumn("score", DataType.INT);
    schema.addColumn("year", DataType.INT);
    testMeta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    StorageManager sm = cluster.getMaster().getStorageManager();
    Appender appender = sm.getTableAppender(testMeta, "test");
    for (int i = 0; i < 10; i++) {
      Tuple t = new VTuple(3);
      t.put(0, DatumFactory.createString("dept"+i));
      t.put(1, DatumFactory.createInt(i+10));
      t.put(2, DatumFactory.createInt(i+1900));
      appender.addTuple(t);
    }
    appender.close();

    testDesc = new TableDescImpl("test", schema, StoreType.CSV,
        new Options(), sm.getTablePath("test"));
    cluster.getMaster().getCatalog().addTable(testDesc);

  }

  @AfterClass
  public static void terminate() throws Exception {
    client.close();
    cluster.shutdown();
  }

  private void assertQueryResult(Map<QueryId, Query> queries) {
    Query q = queries.entrySet().iterator().next().getValue();
    assertEquals(QueryState.QUERY_SUCCEEDED, q.getState());
    SubQuery subQuery = q.getSubQueryIterator().next();
    QueryUnit[] queryUnits = subQuery.getQueryUnits();
    for (QueryUnit queryUnit : queryUnits) {
      for (int i = 0; i <= queryUnit.getRetryCount(); i++) {
        QueryUnitAttempt attempt = queryUnit.getAttempt(i);
        if (i == queryUnit.getRetryCount()) {
          assertEquals(TaskAttemptState.TA_SUCCEEDED,
              attempt.getState());
        } else {
          assertEquals(TaskAttemptState.TA_FAILED,
              attempt.getState());
        }
      }
    }
  }
}
