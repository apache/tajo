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

package org.apache.tajo.thrift.cli;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tajo.SessionVars;
import org.apache.tajo.thrift.cli.TajoThriftCli.TajoThriftCliContext;
import org.apache.tajo.thrift.generated.TGetQueryStatusResponse;
import org.apache.tajo.thrift.generated.TTableDesc;
import org.apache.tajo.thrift.generated.TTableStats;
import org.apache.tajo.util.FileUtil;

import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class DefaultTajoThriftCliOutputFormatter implements TajoThriftCliOutputFormatter {
  private int printPauseRecords;
  private boolean printPause;
  private boolean printErrorTrace;
  private String nullChar;

  @Override
  public void init(TajoThriftCliContext context) {
    this.printPause = context.getBool(SessionVars.CLI_PAGING_ENABLED);
    this.printPauseRecords = context.getInt(SessionVars.CLI_PAGE_ROWS);
    this.printErrorTrace = context.getBool(SessionVars.CLI_DISPLAY_ERROR_TRACE);
    this.nullChar = context.get(SessionVars.CLI_NULL_CHAR);
  }

  @Override
  public void setScirptMode() {
    this.printPause = false;
  }

  private String getQuerySuccessMessage(TTableDesc tableDesc, float responseTime, int totalPrintedRows, String postfix) {
    TTableStats stat = tableDesc.getStats();
    String volume = stat == null ? "0 B" : FileUtil.humanReadableByteCount(stat.getNumBytes(), false);
    long resultRows = stat == null ? 0 : stat.getNumRows();

    long realNumRows = resultRows != 0 ? resultRows : totalPrintedRows;
    return "(" + realNumRows + " rows, " + getResponseTimeReadable(responseTime) + ", " + volume + " " + postfix + ")";
  }

  protected String getResponseTimeReadable(float responseTime) {
    return responseTime + " sec";
  }

  @Override
  public void printResult(PrintWriter sout, InputStream sin, TTableDesc tableDesc,
                          float responseTime, ResultSet res) throws Exception {
    long resultRows = tableDesc.getStats() == null ? 0 : tableDesc.getStats().getNumRows();
    if (resultRows == 0) {
      resultRows = Integer.MAX_VALUE;
    }

    if (res == null) {
      sout.println(getQuerySuccessMessage(tableDesc, responseTime, 0, "inserted"));
      return;
    }
    ResultSetMetaData rsmd = res.getMetaData();
    int numOfColumns = rsmd.getColumnCount();
    for (int i = 1; i <= numOfColumns; i++) {
      if (i > 1) sout.print(",  ");
      String columnName = rsmd.getColumnName(i);
      sout.print(columnName);
    }
    sout.println("\n-------------------------------");

    int numOfPrintedRows = 0;
    int totalPrintedRows = 0;
    while (res.next()) {
      for (int i = 1; i <= numOfColumns; i++) {
        if (i > 1) sout.print(",  ");
        String columnValue = res.getString(i);
        if(res.wasNull()){
          sout.print(nullChar);
        } else {
          sout.print(columnValue);
        }
      }
      sout.println();
      sout.flush();
      numOfPrintedRows++;
      totalPrintedRows++;
      if (printPause && printPauseRecords > 0 && totalPrintedRows < resultRows && numOfPrintedRows >= printPauseRecords) {
        if (resultRows < Integer.MAX_VALUE) {
          sout.print("(" + totalPrintedRows + "/" + resultRows + " rows, continue... 'q' is quit)");
        } else {
          sout.print("(" + totalPrintedRows + " rows, continue... 'q' is quit)");
        }
        sout.flush();
        if (sin != null) {
          if (sin.read() == 'q') {
            sout.println();
            break;
          }
        }
        numOfPrintedRows = 0;
        sout.println();
      }
    }
    sout.println(getQuerySuccessMessage(tableDesc, responseTime, totalPrintedRows, "selected"));
    sout.flush();
  }

  @Override
  public void printNoResult(PrintWriter sout) {
    sout.println("(0 rows)");
    sout.flush();
  }

  @Override
  public void printProgress(PrintWriter sout, TGetQueryStatusResponse status) {
    sout.println("Progress: " + (int)(status.getProgress() * 100.0f)
        + "%, response time: "
        + getResponseTimeReadable((float)((status.getFinishTime() - status.getSubmitTime()) / 1000.0)));
    sout.flush();
  }

  @Override
  public void printMessage(PrintWriter sout, String message) {
    sout.println(message);
    sout.flush();
  }

  @Override
  public void printErrorMessage(PrintWriter sout, Throwable t) {
    sout.println(parseErrorMessage(t.getMessage()));
    if (printErrorTrace) {
      sout.println(ExceptionUtils.getStackTrace(t));
    }
    sout.flush();
  }

  @Override
  public void printErrorMessage(PrintWriter sout, String message) {
    sout.println(parseErrorMessage(message));
    sout.flush();
  }

  @Override
  public void printKilledMessage(PrintWriter sout, String queryId) {
    sout.println(TajoThriftCli.KILL_PREFIX + queryId);
    sout.flush();
  }

  @Override
  public void printErrorMessage(PrintWriter sout, TGetQueryStatusResponse status) {
    if (status.getErrorMessage() != null && !status.getErrorMessage().isEmpty()) {
      printErrorMessage(sout, parseErrorMessage(status.getErrorMessage(), status.getErrorTrace()));
    } else {
      printErrorMessage(sout, "No error message");
    }
    if (printErrorTrace && status.getErrorTrace() != null && !status.getErrorTrace().isEmpty()) {
      sout.println(status.getErrorTrace());
    }
    sout.flush();
  }

  public static String parseErrorMessage(String message) {
    return parseErrorMessage(message, null);
  }

  public static String parseErrorMessage(String message, String trace) {
    if (message == null) {
      return TajoThriftCli.ERROR_PREFIX + "No error message";
    }
    String[] lines = message.split("\n");
    message = lines[0];

    int index = message.lastIndexOf(TajoThriftCli.ERROR_PREFIX);
    if (index < 0) {
      message = TajoThriftCli.ERROR_PREFIX + message;
    } else {
      message = message.substring(index);
    }

    return message;
  }
}
