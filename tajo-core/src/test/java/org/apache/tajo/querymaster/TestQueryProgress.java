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

package org.apache.tajo.querymaster;

import org.apache.tajo.*;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestQueryProgress {
  private static TajoTestingCluster cluster;
  private static TajoConf conf;
  private static TajoClient client;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TpchTestBase.getInstance().getTestingCluster();
    conf = cluster.getConfiguration();
    client = new TajoClientImpl(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.close();
  }

  @Test(timeout = 10000)
  public final void testQueryProgress() throws Exception {
    ClientProtos.SubmitQueryResponse res = client.executeQuery("select l_orderkey from lineitem group by l_orderkey");
    QueryId queryId = new QueryId(res.getQueryId());

    float prevProgress = 0;
    while (true) {
      QueryStatus status = client.getQueryStatus(queryId);
      if (status == null) continue;

      float progress = status.getProgress();

      if (prevProgress > progress) {
        fail("Previous progress: " + prevProgress + ", Current progress : " + progress);
      }
      prevProgress = progress;
      assertTrue(progress <= 1.0f);

      if (TajoClientUtil.isQueryComplete(status.getState())) break;
    }
  }
}
