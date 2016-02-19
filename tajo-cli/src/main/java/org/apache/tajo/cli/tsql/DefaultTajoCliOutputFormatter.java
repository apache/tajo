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
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.util.FileUtil;
import org.fusesource.jansi.Ansi;

import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static com.google.common.base.Strings.repeat;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.fusesource.jansi.Ansi.ansi;
import static org.fusesource.jansi.internal.CLibrary.STDOUT_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

public class DefaultTajoCliOutputFormatter implements TajoCliOutputFormatter {
  private int printPauseRecords;
  private boolean printPause;
  private boolean printErrorTrace;
  private String nullChar;
  public static final char QUIT_COMMAND = 'q';
  public static final boolean REAL_TERMINAL = detectRealTerminal();

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

    int progress = (int)(status.getProgress() * 100.0f);
    String responseTime = getResponseTimeReadable((float)((status.getFinishTime() - status.getSubmitTime()) / 1000.0));
    String progressBar = formatProgressBar(progressWidth, progress);

    reprintProgressLine(sout, progressBar, progress, responseTime, status);
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

  public void reprintProgressLine(PrintWriter out, String progressBar, int progress, String responseTime,
                                  QueryStatus status) {
    // [=====>>                                   ] 10%  3.18 sec
    String lineFormat = "[%s] %d%%  %s";

    if (isRealTerminal()) {
      boolean isLastLine = false;
      if (status.getState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        progressBar = "@|green " + progressBar + "|@";
        isLastLine = true;
      }
      else if (status.getState() == TajoProtos.QueryState.QUERY_ERROR ||
               status.getState() == TajoProtos.QueryState.QUERY_FAILED ||
               status.getState() == TajoProtos.QueryState.QUERY_KILLED) {
        progressBar = "@|red " + progressBar + "|@";
        isLastLine = true;
      }

      String line = String.format(lineFormat, progressBar, progress, responseTime);
      out.print(ansi().eraseLine(Ansi.Erase.ALL).a('\r').render(line));

      if (isLastLine) {
        out.println();
      }
    }
    else {
      String line = String.format(lineFormat, progressBar, progress, responseTime);
      out.println(line);
    }

    out.flush();
  }

  public boolean isRealTerminal() {
    return REAL_TERMINAL;
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

  /**
   * borrowed from Presto
   */
  private static boolean detectRealTerminal() {
    // If the jansi.passthrough property is set, then don't interpret
    // any of the ansi sequences.
    if (Boolean.parseBoolean(System.getProperty("jansi.passthrough"))) {
      return true;
    }

    // If the jansi.strip property is set, then we just strip
    // the ansi escapes.
    if (Boolean.parseBoolean(System.getProperty("jansi.strip"))) {
      return false;
    }

    String os = System.getProperty("os.name");
    if (os.startsWith("Windows")) {
      // We could support this, but we'd need a windows box
      return true;
    }

    // We must be on some unix variant..
    try {
      // check if standard out is a terminal
      if (isatty(STDOUT_FILENO) == 0) {
        return false;
      }
    }
    catch (NoClassDefFoundError | UnsatisfiedLinkError ignore) {
      // These errors happen if the JNI lib is not available for your platform.
    }
    return true;
  }
}
