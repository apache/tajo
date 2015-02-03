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

package org.apache.tajo.cli.tsql;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.*;
import java.net.URL;

import static org.junit.Assert.*;

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
  private ByteArrayOutputStream out;

  @Rule
  public TestName name = new TestName();

  public TestTajoCli() {
    String className = getClass().getSimpleName();
    currentResultPath = new Path(resultBasePath, className);
  }

  @Before
  public void setUp() throws Exception {
    out = new ByteArrayOutputStream();
    tajoCli = new TajoCli(cluster.getConfiguration(), new String[]{}, System.in, out);
  }

  @After
  public void tearDown() throws IOException {
    out.close();
    if (tajoCli != null) {
      tajoCli.close();
    }
  }

  private static void setVar(TajoCli cli, ConfigKey key, String val) throws Exception {
    cli.executeMetaCommand("\\set " + key.keyname() +" " + val);
  }

  private static void assertSessionVar(TajoCli cli, String key, String expectedVal) {
    assertEquals(cli.getContext().getCliSideVar(key), expectedVal);
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
    assertEquals(expectedResult.trim(), actual.trim());
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
    TajoCli testCli = new TajoCli(tajoConf, args, System.in, System.out);
    try {
      assertEquals("false", testCli.getContext().get(SessionVars.CLI_PAGING_ENABLED));
      assertEquals("256", testCli.getContext().getConf().get("tajo.executor.join.inner.in-memory-table-num"));
    } finally {
      testCli.close();
    }
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
    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());
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

    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());
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

    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());
    tajoCli.executeScript(sql);

    String consoleResult = new String(out.toByteArray());
    assertOutputResult(consoleResult);
  }

  private void verifySelectResultWithNullTrue() throws Exception {
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


    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());
    assertSessionVar(tajoCli, SessionVars.CLI_NULL_CHAR.keyname(), "testnull");

    tajoCli.executeScript(sql);

    String consoleResult = new String(out.toByteArray());
    assertOutputResult(consoleResult);
  }

  @Test
  public void testSelectResultWithNullTrueDeprecated() throws Exception {
    setVar(tajoCli, TajoConf.ConfVars.$CLI_NULL_CHAR, "testnull");
    verifySelectResultWithNullTrue();
  }

  @Test
  public void testSelectResultWithNullTrue() throws Exception {
    setVar(tajoCli, SessionVars.CLI_NULL_CHAR, "testnull");
    verifySelectResultWithNullTrue();
  }

  private void verifyStopWhenError() throws Exception {
    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

    assertSessionVar(tajoCli, SessionVars.ON_ERROR_STOP.keyname(), "true");

    tajoCli.executeScript("select count(*) from lineitem; " +
        "select count(*) from lineitem2; " +
        "select count(*) from orders");

    String consoleResult = new String(out.toByteArray());
    assertOutputResult(consoleResult);
  }

  @Test
  public void testGetConf() throws Exception {
    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();
    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TajoCli tajoCli = new TajoCli(tajoConf, new String[]{}, System.in, out);
    try {
      tajoCli.executeMetaCommand("\\getconf tajo.rootdir");

      String consoleResult = new String(out.toByteArray());
      assertEquals(consoleResult, tajoCli.getContext().getConf().getVar(TajoConf.ConfVars.ROOT_DIR) + "\n");
    } finally {
      tajoCli.close();
    }
  }

  @Test
  public void testShowMasters() throws Exception {
    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();
    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TajoCli tajoCli = new TajoCli(tajoConf, new String[]{}, System.in, out);
    tajoCli.executeMetaCommand("\\admin -showmasters");

    String consoleResult = new String(out.toByteArray());

    String masterAddress = tajoCli.getContext().getConf().getVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
    String host = masterAddress.split(":")[0];
    tajoCli.close();
    assertEquals(consoleResult, host + "\n");
  }

  @Test
  public void testStopWhenErrorDeprecated() throws Exception {
    tajoCli.executeMetaCommand("\\set tajo.cli.error.stop true");
    verifyStopWhenError();
  }

  @Test
  public void testStopWhenError() throws Exception {
    tajoCli.executeMetaCommand("\\set ON_ERROR_STOP true");
    verifyStopWhenError();
  }

  @Test
  public void testHelpSessionVars() throws Exception {
    tajoCli.executeMetaCommand("\\help set");
    assertOutputResult(new String(out.toByteArray()));
  }

  @Test(timeout = 3000)
  public void testNonForwardQueryPause() throws Exception {
    final String sql = "select * from default.lineitem";
    TajoCli cli = null;
    try {
      TableDesc tableDesc = cluster.getMaster().getCatalog().getTableDesc("default", "lineitem");
      assertNotNull(tableDesc);
      assertEquals(0L, tableDesc.getStats().getNumRows().longValue());

      InputStream testInput = new ByteArrayInputStream(new byte[]{(byte) DefaultTajoCliOutputFormatter.QUIT_COMMAND});
      cli = new TajoCli(cluster.getConfiguration(), new String[]{}, testInput, out);
      setVar(cli, SessionVars.CLI_PAGE_ROWS, "2");
      setVar(cli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

      cli.executeScript(sql);

      String consoleResult;
      consoleResult = new String(out.toByteArray());
      assertOutputResult(consoleResult);
    } finally {
      cli.close();
    }
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
