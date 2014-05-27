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

package org.apache.tajo.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestTajoCli {
  protected static final TpchTestBase testBase;
  protected static final TajoTestingCluster cluster;

  /** the base path of result directories */
  protected static final Path resultBasePath;
  static {
    testBase = TpchTestBase.getInstance();
    cluster = testBase.getTestingCluster();
    URL resultBaseURL = ClassLoader.getSystemResource("results");
    resultBasePath = new Path(resultBaseURL.toString());
  }

  private TajoCli tajoCli;
  private Path currentResultPath;

  @Rule
  public TestName name = new TestName();

  public TestTajoCli() {
    String className = getClass().getSimpleName();
    currentResultPath = new Path(resultBasePath, className);
  }

  @After
  public void tearDown() {
    if (tajoCli != null) {
      tajoCli.close();
    }
  }

  @Test
  public void testParseParam() throws Exception {
    String[] args = new String[]{"-f", "test.sql", "--param", "test1=10", "--param", "test2=20"};

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(TajoCli.options, args);

    String fileName = cmd.getOptionValue("f");
    assertNotNull(fileName);
    assertEquals(args[1], fileName);

    String[] paramValues = cmd.getOptionValues("param");

    assertNotNull(paramValues);
    assertEquals(2, paramValues.length);

    assertEquals("test1=10", paramValues[0]);
    assertEquals("test2=20", paramValues[1]);
  }

  @Test
  public void testParseConf() throws Exception {
    String[] args = new String[]{"--conf", "tajo.cli.print.pause=false",
        "--conf", "tajo.executor.join.inner.in-memory-table-num=256"};

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(TajoCli.options, args);
    String[] confValues = cmd.getOptionValues("conf");

    assertNotNull(confValues);
    assertEquals(2, confValues.length);

    assertEquals("tajo.cli.print.pause=false", confValues[0]);
    assertEquals("tajo.executor.join.inner.in-memory-table-num=256", confValues[1]);

    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();

    tajoCli = new TajoCli(tajoConf, args, System.in, System.out);
    assertEquals("false", tajoCli.getContext().getConf().get("tajo.cli.print.pause"));
    assertEquals("256", tajoCli.getContext().getConf().get("tajo.executor.join.inner.in-memory-table-num"));
  }

  @Test
  public void testReplaceParam() throws Exception {
    String sql = "select * from lineitem where l_tax > ${tax} and l_returnflag > '${returnflag}'";
    String[] params = new String[]{"tax=10", "returnflag=A"};


    String expected = "select * from lineitem where l_tax > 10 and l_returnflag > 'A'";
    assertEquals(expected, TajoCli.replaceParam(sql, params));
  }

  @Test
  public void testLocalQueryWithoutFrom() throws Exception {
    String sql = "select 'abc', '123'; select substr('123456', 1,3);";
    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();
    tajoConf.setVar(ConfVars.CLI_OUTPUT_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tajoCli = new TajoCli(tajoConf, new String[]{}, System.in, out);
    tajoCli.executeScript(sql);
    String consoleResult = new String(out.toByteArray());

    assertOutputResult(consoleResult);
  }

  @Test
  public void testConnectDatabase() throws Exception {
    String databaseName;

    if (cluster.isHCatalogStoreRunning()) {
      databaseName = "TEST_CONNECTION_DATABASE".toLowerCase();
    } else {
      databaseName = "TEST_CONNECTION_DATABASE";
    }
    String sql = "create database \"" + databaseName + "\";";
    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();
    tajoConf.setVar(ConfVars.CLI_OUTPUT_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tajoCli = new TajoCli(tajoConf, new String[]{}, System.in, out);
    tajoCli.executeScript(sql);

    tajoCli.executeMetaCommand("\\c " + databaseName);
    assertEquals(databaseName, tajoCli.getContext().getCurrentDatabase());

    tajoCli.executeMetaCommand("\\c default");
    assertEquals("default", tajoCli.getContext().getCurrentDatabase());

    tajoCli.executeMetaCommand("\\c \"" + databaseName + "\"");
    assertEquals(databaseName, tajoCli.getContext().getCurrentDatabase());
  }

  @Test
  public void testDescTable() throws Exception {
    String tableName;
    if (cluster.isHCatalogStoreRunning()) {
      tableName = "TEST_DESC_TABLE".toLowerCase();
    } else {
      tableName = "TEST_DESC_TABLE";
    }

    String sql = "create table \"" + tableName + "\" (col1 int4, col2 int4);";

    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();
    tajoConf.setVar(ConfVars.CLI_OUTPUT_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tajoCli = new TajoCli(tajoConf, new String[]{}, System.in, out);
    tajoCli.executeScript(sql);

    tajoCli.executeMetaCommand("\\d " + tableName);
    tajoCli.executeMetaCommand("\\d \"" + tableName + "\"");

    String consoleResult = new String(out.toByteArray());

    FileSystem fs = FileSystem.get(testBase.getTestingCluster().getConfiguration());
    if (!cluster.isHCatalogStoreRunning()) {
      assertOutputResult("testDescTable.result", consoleResult, new String[]{"${table.path}"},
          new String[]{fs.getUri() + "/tajo/warehouse/default/" + tableName});
    }
  }

  @Test
  public void testSelectResultWithNullFalse() throws Exception {
    String sql =
        "select\n" +
            "  c_custkey,\n" +
            "  orders.o_orderkey,\n" +
            "  orders.o_orderstatus \n" +
            "from\n" +
            "  orders full outer join customer on c_custkey = o_orderkey\n" +
            "order by\n" +
            "  c_custkey,\n" +
            "  orders.o_orderkey;\n";

    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();
    tajoConf.setVar(ConfVars.CLI_OUTPUT_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

    tajoConf.setVar(ConfVars.CLI_NULL_CHAR, "");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tajoCli = new TajoCli(tajoConf, new String[]{}, System.in, out);
    tajoCli.executeScript(sql);

    String consoleResult = new String(out.toByteArray());
    assertOutputResult(consoleResult);
  }

  @Test
  public void testSelectResultWithNullTrue() throws Exception {
    String sql =
        "select\n" +
        "  c_custkey,\n" +
        "  orders.o_orderkey,\n" +
        "  orders.o_orderstatus \n" +
        "from\n" +
        "  orders full outer join customer on c_custkey = o_orderkey\n" +
        "order by\n" +
        "  c_custkey,\n" +
        "  orders.o_orderkey;\n";

    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();
    tajoConf.setVar(ConfVars.CLI_OUTPUT_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

    tajoConf.setVar(ConfVars.CLI_NULL_CHAR, "testnull");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tajoCli = new TajoCli(tajoConf, new String[]{}, System.in, out);
    tajoCli.executeScript(sql);

    String consoleResult = new String(out.toByteArray());
    assertOutputResult(consoleResult);
  }

  private void assertOutputResult(String actual) throws Exception {
    assertOutputResult(name.getMethodName() + ".result", actual);
  }

  private void assertOutputResult(String expectedResultFile, String actual) throws Exception {
    assertOutputResult(expectedResultFile, actual, null, null);
  }

  private void assertOutputResult(String expectedResultFile, String actual, String[] paramKeys, String[] paramValues)
      throws Exception {
    FileSystem fs = currentResultPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
    Path resultFile = StorageUtil.concatPath(currentResultPath, expectedResultFile);
    assertTrue(resultFile.toString() + " existence check", fs.exists(resultFile));

    String expectedResult = FileUtil.readTextFile(new File(resultFile.toUri()));

    if (paramKeys != null) {
      for (int i = 0; i < paramKeys.length; i++) {
        if (i < paramValues.length) {
          expectedResult = expectedResult.replace(paramKeys[i], paramValues[i]);
        }
      }
    }
    assertEquals(expectedResult, actual);
  }

  public static class TajoCliOutputTestFormatter extends DefaultTajoCliOutputFormatter {
    @Override
    protected String getResponseTimeReadable(float responseTime) {
      return "";
    }
    @Override
    public void printProgress(PrintWriter sout, QueryStatus status) {
      //nothing to do
    }
  }
}
