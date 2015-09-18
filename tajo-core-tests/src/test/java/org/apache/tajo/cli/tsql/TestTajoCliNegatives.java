/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.cli.tsql;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.SessionVars;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.util.FileUtil;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;

public class TestTajoCliNegatives extends QueryTestCaseBase {
  private static TajoCli tajoCli;
  private static ByteArrayOutputStream out;

  @BeforeClass
  public static void setUp() throws Exception {
    out = new ByteArrayOutputStream();
    tajoCli = new TajoCli(testingCluster.getConfiguration(), new String[]{}, System.in, out);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    out.close();
    if (tajoCli != null) {
      tajoCli.close();
    }
  }

  @Before
  public void resetConsole() throws IOException {
    out.reset();
  }

  public void assertMetaCommandFailure(String cmd, String expectedMsg) throws Exception {
    tajoCli.executeMetaCommand(cmd);
    String consoleResult = new String(out.toByteArray());
    assertEquals(expectedMsg, consoleResult);
  }

  public void assertScriptFailure(String cmd) throws Exception {
    Path resultFile = getResultFile(getMethodName() + ".result");
    String expected = FileUtil.readTextFile(new File(resultFile.toUri()));

    tajoCli.executeScript(cmd);
    String consoleResult = new String(out.toByteArray());
    assertEquals(expected, consoleResult);
  }

  public void assertScriptFailure(String cmd, String expectedMsg) throws Exception {
    tajoCli.executeScript(cmd);
    String consoleResult = new String(out.toByteArray());
    assertEquals(expectedMsg, consoleResult);
  }

  @Test
  public void testConnectDatabase() throws Exception {
    assertMetaCommandFailure("\\c unknown_db", "ERROR: database 'unknown_db' does not exist\n");
  }

  @Test
  public void testDescTable() throws Exception {
    assertMetaCommandFailure("\\d unknown_table", "ERROR: relation 'unknown_table' does not exist\n");
  }

  @Test
  public void testQueryVerification() throws Exception {
    assertScriptFailure("select * from unknown_table", "ERROR: relation 'default.unknown_table' does not exist\n");
  }

  @Test
  public void testQuerySyntax() throws Exception {
    assertScriptFailure("select * from unknown-table");
  }

  private static void setVar(TajoCli cli, ConfigKey key, String val) throws Exception {
    cli.executeMetaCommand("\\set " + key.keyname() + " " + val);
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
  public void testQueryNotImplementedFeature() throws Exception {

    try {
      client.updateQuery("CREATE DATABASE TestTajoCliNegatives");
      client.updateQuery("CREATE TABLE TestTajoCliNegatives.table12u79 ( name RECORD(last TEXT, first TEXT) )");

      assertScriptFailure("select name FROM TestTajoCliNegatives.table12u79",
          "ERROR: not implemented feature: record field projection\n");

    } finally {
      client.updateQuery("DROP TABLE IF EXISTS TestTajoCliNegatives.table12u79");
      client.updateQuery("DROP DATABASE IF EXISTS TestTajoCliNegatives");
    }
  }

  @Test
  public void testQueryFailure() throws Exception {
    setVar(tajoCli, SessionVars.CLI_FORMATTER_CLASS, TajoCliOutputTestFormatter.class.getName());
    assertScriptFailure("select fail(3, l_orderkey, 'testQueryFailure') from default.lineitem" ,
        "ERROR: Internal error. Please check out log files in ${tajo_install_dir}/logs files.\n");
  }
}
