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

import com.google.common.io.NullOutputStream;
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
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.cli.tsql.commands.TajoShellCommand;
import org.apache.tajo.client.ClientParameters;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.rpc.RpcConstants;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.VersionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

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
  private ByteArrayOutputStream err;

  @Rule
  public TestName name = new TestName();

  public TestTajoCli() {
    String className = getClass().getSimpleName();
    currentResultPath = new Path(resultBasePath, className);
  }

  @Before
  public void setUp() throws Exception {
    out = new ByteArrayOutputStream();
    err = new ByteArrayOutputStream();
    Properties connParams = new Properties();
    connParams.setProperty(RpcConstants.CLIENT_RETRY_NUM, "3");
    tajoCli = new TajoCli(cluster.getConfiguration(), new String[]{}, connParams, System.in, out, err);
  }

  @After
  public void tearDown() throws IOException {
    out.close();
    err.close();
    if (tajoCli != null) {
      tajoCli.close();
    }
  }

  private static void setVar(TajoCli cli, ConfigKey key, String val) throws Exception {
    cli.executeMetaCommand("\\set " + key.keyname() + " " + val);
  }

  private static void assertSessionVar(TajoCli cli, String key, String expectedVal) {
    assertEquals(cli.getContext().getCliSideVar(key), expectedVal);
  }

  private void assertOutputResult(String actual) throws Exception {
    assertOutputResult(name.getMethodName() + ".result", actual);
  }

  private void assertErrorResult(String actual, boolean required) throws Exception {
    String fileName = name.getMethodName() + ".err";
    if (required) {
      assertOutputResult(fileName, actual);
    }
  }

  private void assertOutputResult(String expectedResultFile, String actual) throws Exception {
    assertOutputResult(expectedResultFile, actual, null, null);
  }

  private boolean existsFile(String fileName) throws IOException {
    FileSystem fs = currentResultPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
    Path filePath = StorageUtil.concatPath(currentResultPath, fileName);
    return fs.exists(filePath);
  }

  private Path getAbsolutePath(String fileName) {
    return StorageUtil.concatPath(currentResultPath, fileName);
  }

  private void assertOutputResult(String expectedResultFile, String actual, String[] paramKeys, String[] paramValues)
    throws Exception {
    Path path = getAbsolutePath(expectedResultFile);
    assertTrue(path.toString() + " existence check", existsFile(expectedResultFile));

    String expectedResult = FileUtil.readTextFile(new File(path.toUri()));

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
    try (TajoCli testCli = new TajoCli(tajoConf, args, null, System.in, System.out, err)) {
      assertEquals("false", testCli.getContext().get(SessionVars.CLI_PAGING_ENABLED));
      assertEquals("256", testCli.getContext().getConf().get("tajo.executor.join.inner.in-memory-table-num"));
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

  public void verifyConnectDatabase(String testDatabaseName) throws Exception {
    String databaseName;

    if (cluster.isHiveCatalogStoreRunning()) {
      databaseName = testDatabaseName.toLowerCase();
    } else {
      databaseName = testDatabaseName;
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
  public void testConnectDatabase() throws Exception {
    verifyConnectDatabase("TEST_CONNECTION_DATABASE");
  }

  @Test
  public void testConnectDatabaseNamedWithSpace() throws Exception {
    verifyConnectDatabase("TEST CONNECTION DATABASE");
  }

  private void verifyDescTable(String sql, String tableName, String resultFileName) throws Exception {
    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());
    tajoCli.executeScript(sql);

    tajoCli.executeMetaCommand("\\d " + tableName);
    tajoCli.executeMetaCommand("\\d \"" + tableName + "\"");

    String consoleResult = new String(out.toByteArray());

    if (!cluster.isHiveCatalogStoreRunning()) {
      TableMeta meta = cluster.getCatalogService().getTableDesc("default", tableName).getMeta();
      assertOutputResult(resultFileName, consoleResult, new String[]{"${table.timezone}", "${table.path}"},
          new String[]{cluster.getConfiguration().getSystemTimezone().getID(),
              TablespaceManager.getDefault().getTableUri(meta, "default", tableName).toString()});
    }
  }

  public void testDescTable(String testTableName, String resultFileName) throws Exception {
    String tableName;
    if (cluster.isHiveCatalogStoreRunning()) {
      tableName = testTableName.toLowerCase();
    } else {
      tableName = testTableName;
    }

    String sql = "create table \"" + tableName + "\" (col1 int4, col2 int4);";
    verifyDescTable(sql, tableName, resultFileName);
  }

  @Test
  public void testDescTable() throws Exception {
    testDescTable("TEST_DESC_TABLE", "testDescTable1.result");
  }

  @Test
  public void testDescTableNamedWithSpace() throws Exception {
    testDescTable("TEST DESC TABLE", "testDescTable2.result");
  }

  @Test
  public void testDescTableForNestedSchema() throws Exception {
    String tableName;
    if (cluster.isHiveCatalogStoreRunning()) {
      tableName = "TEST_DESC_TABLE_NESTED".toLowerCase();
    } else {
      tableName = "TEST_DESC_TABLE_NESTED";
    }

    String sql = "create table \"" + tableName + "\" (col1 int4, col2 int4, col3 record (col4 record (col5 text)));";
    verifyDescTable(sql, tableName, "testDescTableForNestedSchema.result");
  }

  @Test
  public void testSelectResultWithNullFalse() throws Exception {
    setVar(tajoCli, SessionVars.CLI_NULL_CHAR, "testnull");
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

    String stdoutResult = new String(out.toByteArray());
    assertOutputResult(stdoutResult);
    String stdErrResult = new String(err.toByteArray());
    assertErrorResult(stdErrResult, false);
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
    
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
         ByteArrayOutputStream err = new ByteArrayOutputStream();
         TajoCli tajoCli = new TajoCli(tajoConf, new String[]{}, null, System.in, out, err)) {
      tajoCli.executeMetaCommand("\\getconf tajo.rootdir");

      String consoleResult = new String(out.toByteArray());
      assertEquals(consoleResult, tajoCli.getContext().getConf().getVar(TajoConf.ConfVars.ROOT_DIR) + "\n");
    }
  }

  @Test
  public void testShowMasters() throws Exception {
    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();
    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TajoCli tajoCli = new TajoCli(tajoConf, new String[]{}, null, System.in, out, err);
    tajoCli.executeMetaCommand("\\admin -showmasters");

    String consoleResult = new String(out.toByteArray());

    InetSocketAddress masterAddress =
        tajoCli.getContext().getConf().getSocketAddrVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);

    tajoCli.close();
    assertEquals(consoleResult, masterAddress.getHostName() + "\n");
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
  public void testRunWhenError() throws Exception {
    Thread t = new Thread() {
      public void run() {
        try {
          PipedOutputStream po = new PipedOutputStream();
          InputStream is = new PipedInputStream(po);
          ByteArrayOutputStream out = new ByteArrayOutputStream();

          TajoConf tajoConf = new TajoConf();
          setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());
          Properties connParams = new Properties();
          connParams.setProperty(ClientParameters.RETRY, "3");
          TajoCli tc = new TajoCli(tajoConf, new String[]{}, connParams, is, out, err);

          tc.executeMetaCommand("\\set ON_ERROR_STOP false");
          assertSessionVar(tc, SessionVars.ON_ERROR_STOP.keyname(), "false");

          po.write(new String("asdf;\nqwe;\nzxcv;\n").getBytes());

          tc.runShell();
        } catch (Exception e) {
          throw new RuntimeException("Cannot run thread in testRunWhenError", e);
        }
      }
    };

    t.start();
    Thread.sleep(1000);
    if(!t.isAlive()) {
      fail("TSQL should be alive");
    } else {
      t.interrupt();
      t.join();
    }
  }

  @Test
  public void testHelpSessionVars() throws Exception {
    tajoCli.executeMetaCommand("\\help set");
    assertOutputResult(new String(out.toByteArray()));
  }

  @Test
  public void testTimeZoneSessionVars1() throws Exception {
    tajoCli.executeMetaCommand("\\set TIMEZONE GMT+1");
    tajoCli.executeMetaCommand("\\set");
    String output = new String(out.toByteArray());
    assertTrue(output.contains("'TIMEZONE'='GMT+1'"));
  }

  @Test
  public void testTimeZoneSessionVars2() throws Exception {
    tajoCli.executeScript("SET TIME ZONE 'GMT+2'");
    tajoCli.executeMetaCommand("\\set");
    String output = new String(out.toByteArray());
    assertTrue(output.contains("'TIMEZONE'='GMT+2'"));
  }

  @Test
  public void testTimeZoneSessionVars3() throws Exception {
    tajoCli.executeMetaCommand("\\set timezone GMT+1");
    tajoCli.executeMetaCommand("\\set");
    String output = new String(out.toByteArray());
    assertTrue(output.contains("'TIMEZONE'='GMT+1'"));
  }

  @Test
  public void testTimeZoneSessionVars4() throws Exception {
    tajoCli.executeMetaCommand("\\set timeZone GMT+2");
    tajoCli.executeMetaCommand("\\set");
    String output = new String(out.toByteArray());
    assertTrue(output.contains("'TIMEZONE'='GMT+2'"));
  }

  @Test
  public void testTimeZoneTest1() throws Exception {
    String tableName = "test1";
    tajoCli.executeMetaCommand("\\set TIMEZONE GMT+0");
    tajoCli.executeScript("create table " + tableName + " (col1 TIMESTAMP)");
    tajoCli.executeScript("insert into " + tableName + " select to_timestamp(0)");
    out.reset();

    tajoCli.executeScript("select * from " + tableName);
    String consoleResult = new String(out.toByteArray());
    tajoCli.executeScript("DROP TABLE " + tableName + " PURGE");
    assertEquals("1970-01-01 00:00:00", consoleResult.split("\n")[2]);
  }

  @Test
  public void testTimeZoneTest2() throws Exception {
    String tableName = "test1";
    tajoCli.executeMetaCommand("\\set TIMEZONE GMT+1");
    tajoCli.executeScript("create table " + tableName + " (col1 TIMESTAMP)");
    tajoCli.executeScript("insert into " + tableName + " select to_timestamp(0)");
    out.reset();

    tajoCli.executeScript("select * from " + tableName);
    String consoleResult = new String(out.toByteArray());
    tajoCli.executeScript("DROP TABLE " + tableName + " PURGE");
    assertEquals("1970-01-01 01:00:00", consoleResult.split("\n")[2]);
  }

  @Test(timeout = 3000)
  public void testNonForwardQueryPause() throws Exception {
    final String sql = "select * from default.lineitem";
    TableDesc tableDesc = cluster.getMaster().getCatalog().getTableDesc("default", "lineitem");
    assertNotNull(tableDesc);
    assertEquals(0L, tableDesc.getStats().getNumRows().longValue());

    try (InputStream testInput = new ByteArrayInputStream(new byte[]{(byte) DefaultTajoCliOutputFormatter.QUIT_COMMAND});
         TajoCli cli = new TajoCli(cluster.getConfiguration(), new String[]{}, null, testInput, out, err)) {
      setVar(cli, SessionVars.CLI_PAGE_ROWS, "2");
      setVar(cli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());

      cli.executeScript(sql);

      String consoleResult;
      consoleResult = new String(out.toByteArray());
      assertOutputResult(consoleResult);
    }
  }

  @Test
  public void testResultRowNumWhenSelectingOnPartitionedTable() throws Exception {
    try (TajoCli cli2 = new TajoCli(cluster.getConfiguration(), new String[]{}, null, System.in,
        new NullOutputStream(), new NullOutputStream())) {
      cli2.executeScript("create table region_part (r_regionkey int8, r_name text) " +
          "partition by column (r_comment text) as select * from region");

      setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());
      tajoCli.executeScript("select r_comment from region_part where r_comment = 'hs use ironic, even requests. s'");
      String consoleResult = new String(out.toByteArray());
      assertOutputResult(consoleResult);
    } finally {
      tajoCli.executeScript("drop table region_part purge");
    }
  }

  // TODO: This should be removed at TAJO-1891
  @Test
  public void testAddPartitionNotimplementedException() throws Exception {
    String tableName = IdentifierUtil.normalizeIdentifier("testAddPartitionNotimplementedException");
    tajoCli.executeScript("create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8)");
    tajoCli.executeScript("alter table " + tableName + " add partition (key2 = 0.1)");

    String consoleResult;
    consoleResult = new String(out.toByteArray());
    assertOutputResult(consoleResult);
  }

  // TODO: This should be added at TAJO-1891
  public void testAlterTableAddDropPartition() throws Exception {
    String tableName = IdentifierUtil.normalizeIdentifier("testAlterTableAddPartition");

    tajoCli.executeScript("create table " + tableName + " (col1 int4, col2 int4) partition by column(key float8)");
    tajoCli.executeScript("alter table " + tableName + " add partition (key2 = 0.1)");
    tajoCli.executeScript("alter table " + tableName + " add partition (key = 0.1)");
    tajoCli.executeScript("alter table " + tableName + " drop partition (key = 0.1)");
    tajoCli.executeScript("alter table " + tableName + " drop partition (key = 0.1)");

    tajoCli.executeScript("drop table " + tableName);
    tajoCli.executeScript("create table " + tableName
      + " (col1 int4, col2 int4) partition by column(col3 float8, col4 int4)");

    TajoClient client = testBase.getTestingCluster().newTajoClient();
    TableDesc tableDesc = client.getTableDesc(tableName);

    String partitionLocation = tableDesc.getUri().toString() + "/col5=0.1/col6=10";
    tajoCli.executeScript("alter table " + tableName + " add partition (col3 = 0.1, col4 = 10)"
      + " location '" + partitionLocation + "'");

    Path partitionPath = new Path(partitionLocation);
    FileSystem fs = testBase.getTestingCluster().getDefaultFileSystem();
    assertTrue(fs.exists(partitionPath));

    tajoCli.executeScript("alter table " + tableName + " drop partition (col3 = 0.1, col4 = 10)");

    String consoleResult = new String(out.toByteArray());
    assertOutputResult(consoleResult);
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

  @Test
  public void testPrintVersion() {
    tajoCli.executeMetaCommand("\\?");
    String consoleResult = new String(out.toByteArray());
    String tajoFullVersion = VersionInfo.getVersion();
    String tajoVersion;

    int delimiterIdx = tajoFullVersion.indexOf("-");
    if (delimiterIdx > -1) {
      tajoVersion = tajoFullVersion.substring(0, delimiterIdx);
    } else {
      tajoVersion = tajoFullVersion;
    }

    if (tajoVersion.equalsIgnoreCase("") || tajoFullVersion.contains("SNAPSHOT")) {
      assertTrue(consoleResult.contains("docs/current/"));
    } else {
      assertTrue(consoleResult.contains("docs/" + tajoVersion + "/"));
    }
  }

  @Test
  public void testDefaultPrintHelp() throws IOException, NoSuchMethodException {
    for (Map.Entry<String, TajoShellCommand> entry : tajoCli.getContext().getCommands().entrySet()) {
      TajoShellCommand shellCommand = entry.getValue();

      if (!shellCommand.getClass().getMethod("printHelp").getDeclaringClass().equals(shellCommand.getClass())) {
        tajoCli.executeMetaCommand("\\help " + entry.getKey().replace("\\", ""));
        String result = new String(out.toByteArray());
        out.reset();

        String expected = shellCommand.getCommand()
                + " " + shellCommand.getUsage()
                + " - " + shellCommand.getDescription() + "\n";

        assertEquals(result, expected);
      }
    }
  }

  @Test
  public void testPrintUsageOfConnectDatabaseCommand() {
    tajoCli.executeMetaCommand("\\help c");
    assertTrue(new String(out.toByteArray()).contains("[database_name]"));
  }

  @Test
  public void testPrintUsageOfSetCommand() {
    tajoCli.executeMetaCommand("\\set a b c");
    assertTrue(new String(out.toByteArray()).contains("[[NAME] VALUE]"));
  }

  @Test
  public void testPrintUsageOfUnsetCommand() {
    tajoCli.executeMetaCommand("\\help unset");
    assertTrue(new String(out.toByteArray()).contains("[NAME]"));
  }
}
