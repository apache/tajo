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

import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Float8Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.jdbc.MetaDataTuple;
import org.apache.tajo.jdbc.TajoMetaDataResultSet;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDefaultCliOutputFormatter {
  @Test
  public void testParseErrorMessage() {
    String message = "java.sql.SQLException: ERROR: no such a table: table1";
    assertEquals("ERROR: no such a table: table1", DefaultTajoCliOutputFormatter.parseErrorMessage(message));

    String multiLineMessage =
        "ERROR: java.sql.SQLException: ERROR: no such a table: table1\n" +
        "com.google.protobuf.ServiceException: java.sql.SQLException: ERROR: no such a table: table1\n" +
        "\tat org.apache.tajo.rpc.ServerCallable.withRetries(ServerCallable.java:107)\n" +
        "\tat org.apache.tajo.client.TajoClient.getTableDesc(TajoClient.java:777)\n" +
        "\tat org.apache.tajo.cli.DescTableCommand.invoke(DescTableCommand.java:43)\n" +
        "\tat org.apache.tajo.cli.TajoCli.executeMetaCommand(TajoCli.java:300)\n" +
        "\tat org.apache.tajo.cli.TajoCli.executeParsedResults(TajoCli.java:280)\n" +
        "\tat org.apache.tajo.cli.TajoCli.runShell(TajoCli.java:271)\n" +
        "\tat org.apache.tajo.cli.TajoCli.main(TajoCli.java:420)\n" +
        "Caused by: java.sql.SQLException: ERROR: no such a table: table1\n" +
        "\tat org.apache.tajo.client.TajoClient$22.call(TajoClient.java:791)\n" +
        "\tat org.apache.tajo.client.TajoClient$22.call(TajoClient.java:778)\n" +
        "\tat org.apache.tajo.rpc.ServerCallable.withRetries(ServerCallable.java:97)\n" +
        "\t... 6 more";

    assertEquals("ERROR: no such a table: table1", DefaultTajoCliOutputFormatter.parseErrorMessage(multiLineMessage));
  }

  @Test
  public void testPrintResultInsertStatement() throws Exception {
    TajoConf tajoConf = new TajoConf();
    DefaultTajoCliOutputFormatter outputFormatter = new DefaultTajoCliOutputFormatter();
    outputFormatter.init(tajoConf);

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
    TajoConf tajoConf = new TajoConf();
    DefaultTajoCliOutputFormatter outputFormatter = new DefaultTajoCliOutputFormatter();
    outputFormatter.init(tajoConf);

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
