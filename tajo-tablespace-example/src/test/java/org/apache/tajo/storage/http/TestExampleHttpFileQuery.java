/*
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

package org.apache.tajo.storage.http;

import net.minidev.json.JSONObject;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.storage.TablespaceManager;
import org.junit.*;

import java.net.InetAddress;
import java.net.URI;

public class TestExampleHttpFileQuery extends QueryTestCaseBase {

  private static ExampleHttpTablespaceTestServer server;

  @BeforeClass
  public static void setup() throws Exception {
    server = new ExampleHttpTablespaceTestServer();
    server.init();

    JSONObject configElements = new JSONObject();
    URI uri = URI.create("http://" + InetAddress.getLocalHost().getHostName() + ":" + server.getAddress().getPort());
    TablespaceManager.addTableSpaceForTest(new ExampleHttpFileTablespace("http_example", uri, configElements));

    QueryTestCaseBase.testingCluster.getMaster().refresh();
  }

  @AfterClass
  public static void teardown() throws Exception {
    server.close();
  }

  @Before
  public void prepareTables() throws TajoException {
    executeString("create table got (*) tablespace http_example using ex_http_json with ('path'='got.json')");
    executeString("create table github (*) tablespace http_example using ex_http_json with ('path'='github.json')");
  }

  @After
  public void dropTables() throws TajoException {
    executeString("drop table got");
    executeString("drop table github");
  }

  @SimpleTest
  @Test
  public void testSelect() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testGroupby() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testSort() throws Exception {
    runSimpleTests();
  }

  @SimpleTest
  @Test
  public void testJoin() throws Exception {
    runSimpleTests();
  }
}
