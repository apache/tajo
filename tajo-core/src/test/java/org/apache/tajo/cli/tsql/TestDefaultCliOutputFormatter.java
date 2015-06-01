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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.cli.tsql.DefaultTajoCliOutputFormatter;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Float8Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.jdbc.MetaDataTuple;
import org.apache.tajo.jdbc.TajoMetaDataResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDefaultCliOutputFormatter {
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

  private TajoConf conf;
  private TajoCli tajoCli;
  private TajoCli.TajoCliContext cliContext;

  @Before
  public void setUp() throws Exception {
    conf = cluster.getConfiguration();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tajoCli = new TajoCli(conf, new String[]{}, System.in, out);
    cliContext = tajoCli.getContext();
  }

  @After
  public void tearDown() {
    if (tajoCli != null) {
      tajoCli.close();
    }
  }


  @Test
  public void testParseErrorMessage() {
    String message = "java.sql.SQLException: ERROR: no such a table: table1";
    assertEquals("ERROR: no such a table: table1", DefaultTajoCliOutputFormatter.parseErrorMessage(message));

    String multiLineMessage =
        "ERROR: java.sql.SQLException: ERROR: no such a table: table1\n" +
        "com.google.protobuf.ServiceException: java.sql.SQLException: ERROR: no such a table: table1\n" +
        "\tat org.apache.tajo.client.TajoClient.getTableDesc(TajoClient.java:777)\n" +
        "\tat org.apache.tajo.cli.tsql.commands.DescTableCommand.invoke(DescTableCommand.java:43)\n" +
        "\tat org.apache.tajo.cli.tsql.TajoCli.executeMetaCommand(TajoCli.java:300)\n" +
        "\tat org.apache.tajo.cli.tsql.TajoCli.executeParsedResults(TajoCli.java:280)\n" +
        "\tat org.apache.tajo.cli.tsql.TajoCli.runShell(TajoCli.java:271)\n" +
        "\tat org.apache.tajo.cli.tsql.TajoCli.main(TajoCli.java:420)\n" +
        "Caused by: java.sql.SQLException: ERROR: no such a table: table1\n" +
        "\t... 6 more";

    assertEquals(multiLineMessage, DefaultTajoCliOutputFormatter.parseErrorMessage(multiLineMessage));

    String noPrefixMessage = "RTFM please";
    assertEquals("ERROR: "+noPrefixMessage, DefaultTajoCliOutputFormatter.parseErrorMessage(noPrefixMessage));

    String errorMessageWithLine = "ERROR: syntax error at or near '('\n" +
        "LINE 1:7 select (*) from tc\n" +
        "                ^";
    assertEquals(errorMessageWithLine, DefaultTajoCliOutputFormatter.parseErrorMessage(errorMessageWithLine));
  }

  @Test
  public void testPrintResultInsertStatement() throws Exception {
    DefaultTajoCliOutputFormatter outputFormatter = new DefaultTajoCliOutputFormatter();
    outputFormatter.init(cliContext);

    float responseTime = 10.1f;
    long numBytes = 102;
    long numRows = 30;

    TableDesc tableDesc = new TableDesc();
    TableStats stats = new TableStats();
    stats.setNumBytes(102);
    stats.setNumRows(numRows);
    tableDesc.setStats(stats);

    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter);
    outputFormatter.printResult(writer, null, tableDesc, responseTime, null);

    String expectedOutput = "(" +  numRows + " rows, " + responseTime + " sec, " + numBytes + " B inserted)\n";
    assertEquals(expectedOutput, stringWriter.toString());
  }

  @Test
  public void testPrintResultSelectStatement() throws Exception {
    DefaultTajoCliOutputFormatter outputFormatter = new DefaultTajoCliOutputFormatter();
    outputFormatter.init(cliContext);

    float responseTime = 10.1f;
    long numBytes = 102;
    long numRows = 30;

    TableDesc tableDesc = new TableDesc();
    TableStats stats = new TableStats();
    stats.setNumBytes(102);
    stats.setNumRows(numRows);
    tableDesc.setStats(stats);

    final List<MetaDataTuple> resultTables = new ArrayList<MetaDataTuple>();

    String expectedOutput = "col1,  col2,  col3\n";
    expectedOutput += "-------------------------------\n";

    String prefix = "";
    for (int i = 0; i < numRows; i++) {
      MetaDataTuple tuple = new MetaDataTuple(3);

      int index = 0;

      tuple.put(index++, new TextDatum("row_" + i));
      tuple.put(index++, new Int4Datum(i));
      tuple.put(index++, new Float8Datum(i));

      expectedOutput += prefix + "row_" + i + ",  " + (new Int4Datum(i)) + ",  " + (new Float8Datum(i));
      prefix = "\n";
      resultTables.add(tuple);
    }
    expectedOutput += "\n(" +  numRows + " rows, " + responseTime + " sec, " + numBytes + " B selected)\n";

    ResultSet resultSet = new TajoMetaDataResultSet(
        Arrays.asList("col1", "col2", "col3"),
        Arrays.asList(TajoDataTypes.Type.TEXT, TajoDataTypes.Type.INT4, TajoDataTypes.Type.FLOAT8),
        resultTables);

    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter);
    outputFormatter.printResult(writer, null, tableDesc, responseTime, resultSet);

    assertEquals(expectedOutput, stringWriter.toString());
  }
}
