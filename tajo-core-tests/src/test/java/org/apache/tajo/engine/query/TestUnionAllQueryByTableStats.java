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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatsProto;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.ipc.ClientProtos.GetQueryResultResponse;
import org.apache.tajo.ipc.ClientProtos.SubmitQueryResponse;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.*;

public class TestUnionAllQueryByTableStats {
  protected static final TpchTestBase testBase;
  protected static final TajoTestingCluster cluster;

  /** the base path of query directories */
  protected static Path queryBasePath;

  static {
    testBase = TpchTestBase.getInstance();
    cluster = testBase.getTestingCluster();

    URL queryBaseURL = ClassLoader.getSystemResource("queries");
    Preconditions.checkNotNull(queryBaseURL, "queries directory is absent.");
    queryBasePath = new Path(queryBaseURL.toString());
  }

  private TajoClient client;
  protected Path namedQueryPath;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    client = cluster.newTajoClient();

    NamedTest namedTest = getClass().getAnnotation(NamedTest.class);
    if (namedTest != null) {
      namedQueryPath = new Path(queryBasePath, namedTest.value());
    }
  }

  @After
  public void tearDown() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testUnionAll1() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(8L, stats.getNumRows());
    assertEquals(96L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll2() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(10L, stats.getNumRows());
    assertEquals(120L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll3() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(2L, stats.getNumRows());
    assertEquals(32L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll4() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(1L, stats.getNumRows());
    assertEquals(16L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll5() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(1L, stats.getNumRows());
    assertEquals(16L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll6() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(1L, stats.getNumRows());
    assertEquals(16L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll7() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(10L, stats.getNumRows());
    assertEquals(120L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll8() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(1L, stats.getNumRows());
    assertEquals(22L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll9() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(5L, stats.getNumRows());
    assertEquals(137L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll10() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(20L, stats.getNumRows());
    assertEquals(548L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll11() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(1L, stats.getNumRows());
    assertEquals(44L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll12() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(5L, stats.getNumRows());
    assertEquals(414L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll13() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(5L, stats.getNumRows());
    assertEquals(414L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll14() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(7L, stats.getNumRows());
    assertEquals(175L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll15() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(3L, stats.getNumRows());
    assertEquals(75L, stats.getNumBytes());
  }

  @Test
  public void testUnionAll16() throws Exception {
    String query = getQueryByClassName(TestUnionQuery.class.getSimpleName());

    SubmitQueryResponse response = client.executeQuery(query);
    assertNotNull(response);

    QueryId queryId = new QueryId(response.getQueryId());
    GetQueryResultResponse resultResponse = getQueryResultResponse(queryId);

    TableDescProto desc = resultResponse.getTableDesc();
    assertNotNull(desc);

    TableStatsProto stats = desc.getStats();
    assertNotNull(stats);

    assertEquals(3L, stats.getNumRows());
    assertEquals(75L, stats.getNumBytes());
  }

  private GetQueryResultResponse getQueryResultResponse(QueryId queryId) throws Exception {
    while (true) {
      QueryStatus status = client.getQueryStatus(queryId);
      if (status == null) continue;
      if (TajoClientUtil.isQueryComplete(status.getState())) break;
    }

    GetQueryResultResponse resultResponse = client.getResultResponse(queryId);
    assertNotNull(resultResponse);

    return resultResponse;
  }

  private String getQueryByClassName(String className) throws Exception {
    String queryFileName = getMethodName() + ".sql";
    Path queryFilePath = getQueryFilePath(className, queryFileName);
    return FileUtil.readTextFile(new File(queryFilePath.toUri()));
  }

  private Path getQueryFilePath(String className, String fileName) throws IOException {
    Path currentQueryPath = new Path(queryBasePath, className);

    Path queryFilePath = StorageUtil.concatPath(currentQueryPath, fileName);
    FileSystem fs = currentQueryPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
    if (!fs.exists(queryFilePath)) {
      if (namedQueryPath != null) {
        queryFilePath = StorageUtil.concatPath(namedQueryPath, fileName);
        fs = namedQueryPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
        if (!fs.exists(queryFilePath)) {
          throw new IOException("Cannot find " + fileName + " at " + currentQueryPath + " and " + namedQueryPath);
        }
      } else {
        throw new IOException("Cannot find " + fileName + " at " + currentQueryPath);
      }
    }
    return queryFilePath;
  }

  private String getMethodName() {
    String methodName = name.getMethodName();
    // In the case of parameter execution name's pattern is methodName[0]
    if (methodName.endsWith("]")) {
      methodName = methodName.substring(0, methodName.length() - 3);
    }
    return methodName;
  }
}
