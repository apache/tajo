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

import jline.TerminalFactory;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.util.FileUtil;

import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static com.google.common.base.Strings.repeat;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class DefaultTajoCliOutputFormatter implements TajoCliOutputFormatter {
  private int printPauseRecords;
  private boolean printPause;
  private boolean printErrorTrace;
  private String nullChar;
  public static final char QUIT_COMMAND = 'q';

  @Override
  public void init(TajoCli.TajoCliContext context) {
    this.printPause = context.getBool(SessionVars.CLI_PAGING_ENABLED);
    this.printPauseRecords = context.getInt(SessionVars.CLI_PAGE_ROWS);
    this.printErrorTrace = context.getBool(SessionVars.CLI_DISPLAY_ERROR_TRACE);
    this.nullChar = context.get(SessionVars.CLI_NULL_CHAR);
  }

  @Override
  public void setScriptMode() {
    this.printPause = false;
  }

  private String getQuerySuccessMessage(TableDesc tableDesc, float responseTime, int totalPrintedRows, String postfix,
                                        boolean endOfTuple) {
    TableStats stat = tableDesc.getStats();
    String volume = stat == null ? (endOfTuple ? "0 B" : "unknown bytes") :
        FileUtil.humanReadableByteCount(stat.getNumBytes(), false);
    long resultRows = stat == null ? TajoConstants.UNKNOWN_ROW_NUMBER : stat.getNumRows();

    String displayRowNum;
    if (resultRows == TajoConstants.UNKNOWN_ROW_NUMBER) {

      if (endOfTuple) {
        displayRowNum = totalPrintedRows + " rows";
      } else {
        displayRowNum = "unknown row number";
      }

    } else {
      displayRowNum = resultRows + " rows";
    }
    return "(" + displayRowNum + ", " + getResponseTimeReadable(responseTime) + ", " + volume + " " + postfix + ")";
  }

  protected String getResponseTimeReadable(float responseTime) {
    return responseTime + " sec";
  }

  @Override
  public void printResult(PrintWriter sout, InputStream sin, TableDesc tableDesc,
                          float responseTime, ResultSet res) throws Exception {
    long resultRows = tableDesc.getStats() == null ? -1 : tableDesc.getStats().getNumRows();
    if (resultRows == -1) {
      resultRows = Integer.MAX_VALUE;
    }

    if (res == null) {
      sout.println(getQuerySuccessMessage(tableDesc, responseTime, 0, "inserted", true));
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
    boolean endOfTuple = true;
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
          if (sin.read() == QUIT_COMMAND) {
            endOfTuple = false;
            sout.println();
            break;
          }
        }
        numOfPrintedRows = 0;
        sout.println();
      }
    }
    sout.println(getQuerySuccessMessage(tableDesc, responseTime, totalPrintedRows, "selected", endOfTuple));
    sout.flush();
  }

  @Override
  public void printNoResult(PrintWriter sout) {
    sout.println("(0 rows)");
    sout.flush();
  }

  @Override
  public void printProgress(PrintWriter sout, QueryStatus status) {
    int terminalWidth = TerminalFactory.get().getWidth();
    int progressWidth = (min(terminalWidth, 100) - 75) + 17; // progress bar is 17-42 characters wide

    String progressBar = formatProgressBar(progressWidth, (int)(status.getProgress() * 100.0f));

    // 0:17 [=====>>                                   ] 10%
    String progressLine = String.format("%s [%s] %d%%",
      getResponseTimeReadable((float)((status.getFinishTime() - status.getSubmitTime()) / 1000.0)),
      progressBar,
      (int)(status.getProgress() * 100.0f));

    sout.print('\r' + progressLine);
    sout.flush();
  }

  public String formatProgressBar(int width, int progress) {
    if (progress == 0) {
      return repeat(" ", width);
    }

    // compute nominal lengths
    int completeLength = min(width, ceil(progress * width, 100));
    int remainLength = width;
    int runningLength = 1;

    // adjust to fix rounding errors
    if (((completeLength + runningLength + remainLength) != width) && (remainLength > 0)) {
      remainLength = max(0, width - completeLength - runningLength);
    }

    if (((completeLength + runningLength + remainLength) > width) && (progress > 0)) {
      completeLength = max(0, width - runningLength - remainLength);
    }

    return repeat("=", completeLength) + repeat(">", runningLength) + repeat(" ", remainLength);
  }

  private int ceil(int dividend, int divisor) {
    return ((dividend + divisor) - 1) / divisor;
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
  public void printKilledMessage(PrintWriter sout, QueryId queryId) {
    sout.println(TajoCli.KILL_PREFIX + queryId);
    sout.flush();
  }

  @Override
  public void printErrorMessage(PrintWriter sout, QueryStatus status) {
    if (status.getErrorMessage() != null && !status.getErrorMessage().isEmpty()) {
      printErrorMessage(sout, parseErrorMessage(status.getErrorMessage()));
    } else {
      printErrorMessage(sout, "No error message");
    }
    if (printErrorTrace && status.getErrorTrace() != null && !status.getErrorTrace().isEmpty()) {
      sout.println(status.getErrorTrace());
    }
    sout.flush();
  }

  public static String parseErrorMessage(String message) {
    if (message == null) {
      return TajoCli.ERROR_PREFIX + "No error message";
    }

    int index = message.indexOf(TajoCli.ERROR_PREFIX);
    if (index < 0) {
      message = TajoCli.ERROR_PREFIX + message;
    } else {
      message = message.substring(index);
    }

    return message;
  }
}
