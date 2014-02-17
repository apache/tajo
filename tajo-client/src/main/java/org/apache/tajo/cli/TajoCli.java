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

import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import jline.console.history.PersistentHistory;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.jdbc.TajoResultSet;
import org.apache.tajo.util.FileUtil;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Map;
import java.util.TreeMap;

public class TajoCli {
  public static final int PRINT_LIMIT = 24;

  private final TajoConf conf;
  private static final Options options;

  private TajoClient client;

  private final ConsoleReader reader;
  private final InputStream sin;
  private final PrintWriter sout;

  private final Map<String, TajoShellCommand> commands = new TreeMap<String, TajoShellCommand>();

  private static final Class [] registeredCommands = {
      DescTableCommand.class,
      DescFunctionCommand.class,
      HelpCommand.class,
      ExitCommand.class,
      CopyrightCommand.class,
      VersionCommand.class
  };

  private static final String HOME_DIR = System.getProperty("user.home");
  private static final String HISTORY_FILE = ".tajo_history";

  static {
    options = new Options();
    options.addOption("c", "command", true, "execute only single command, then exit");
    options.addOption("f", "file", true, "execute commands from file, then exit");
    options.addOption("h", "host", true, "Tajo server host");
    options.addOption("p", "port", true, "Tajo server port");
  }

  public TajoCli(TajoConf c, String [] args,
                 InputStream in, OutputStream out) throws Exception {
    this.conf = new TajoConf(c);
    this.sin = in;
    this.reader = new ConsoleReader(sin, out);
    this.sout = new PrintWriter(reader.getOutput());

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    String hostName = null;
    Integer port = null;
    if (cmd.hasOption("h")) {
      hostName = cmd.getOptionValue("h");
    }
    if (cmd.hasOption("p")) {
      port = Integer.parseInt(cmd.getOptionValue("p"));
    }

    // if there is no "-h" option,
    if(hostName == null) {
      if (conf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS) != null) {
        // it checks if the client service address is given in configuration and distributed mode.
        // if so, it sets entryAddr.
        hostName = conf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS).split(":")[0];
      }
    }
    if (port == null) {
      if (conf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS) != null) {
        // it checks if the client service address is given in configuration and distributed mode.
        // if so, it sets entryAddr.
        port = Integer.parseInt(conf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS).split(":")[1]);
      }
    }

    if ((hostName == null) ^ (port == null)) {
      System.err.println("ERROR: cannot find valid Tajo server address");
      System.exit(-1);
    } else if (hostName != null && port != null) {
      conf.setVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, hostName+":"+port);
      client = new TajoClient(conf);
    } else if (hostName == null && port == null) {
      client = new TajoClient(conf);
    }

    initHistory();
    initCommands();

    if (cmd.hasOption("c")) {
      executeStatements(cmd.getOptionValue("c"));
      sout.flush();
      System.exit(0);
    }
    if (cmd.hasOption("f")) {
      File sqlFile = new File(cmd.getOptionValue("f"));
      if (sqlFile.exists()) {
        String contents = FileUtil.readTextFile(new File(cmd.getOptionValue("f")));
        executeStatements(contents);
        sout.flush();
        System.exit(0);
      } else {
        System.err.println("No such a file \"" + cmd.getOptionValue("f") + "\"");
        System.exit(-1);
      }
    }
  }

  private void initHistory() {
    try {
      String historyPath = HOME_DIR + File.separator + HISTORY_FILE;
      if ((new File(HOME_DIR)).exists()) {
        reader.setHistory(new FileHistory(new File(historyPath)));
      } else {
        System.err.println("ERROR: home directory : '" + HOME_DIR +"' does not exist.");
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  private void initCommands() {
    for (Class clazz : registeredCommands) {
      TajoShellCommand cmd = null;
      try {
         Constructor cons = clazz.getConstructor(new Class[] {TajoClient.class, PrintWriter.class});
         cmd = (TajoShellCommand) cons.newInstance(client, sout);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        System.exit(-1);
      }
      commands.put(cmd.getCommand(), cmd);
    }
  }

  public int runShell() throws Exception {

    String raw;
    String line;
    StringBuffer accumulatedLine = new StringBuffer();
    String prompt = "tajo";
    String curPrompt = prompt;
    boolean newStatement = true;
    int code = 0;

    sout.write("Try \\? for help.\n");
    while((raw = reader.readLine(curPrompt + "> ")) != null) {
      // each accumulated line has a space delimiter
      if (accumulatedLine.length() > 0) {
        accumulatedLine.append(' ');
      }

      line = raw.trim();

      if (line.length() == 0) { // if empty line
        continue;

      } else if (line.charAt(0) == '/') { // warning for legacy usage
        printInvalidCommand(line);
        continue;

      } else if (line.charAt(0) == '\\') { // command mode
        ((PersistentHistory)reader.getHistory()).flush();
        executeCommand(line);

      } else if (line.endsWith(";") && !line.endsWith("\\;")) {

        // remove a trailing newline
        line = StringUtils.chomp(line).trim();

        // get a punctuated statement
        String punctuated = accumulatedLine + line;

        if (!newStatement) {
          // why do two lines are removed?
          // First history line indicates an accumulated line.
          // Second history line is a just typed line.
          reader.getHistory().removeLast();
          reader.getHistory().removeLast();
          reader.getHistory().add(punctuated);
          ((PersistentHistory)reader.getHistory()).flush();
        }

        code = executeStatements(punctuated);

        // reset accumulated lines
        newStatement = true;
        accumulatedLine = new StringBuffer();
        curPrompt = prompt;

      } else {
        line = StringUtils.chomp(raw).trim();

        // accumulate a line
        accumulatedLine.append(line);

        // replace the latest line with a accumulated line
        if (!newStatement) { // if this is not first line, remove one more line.
          reader.getHistory().removeLast();
        } else {
          newStatement = false;
        }
        reader.getHistory().removeLast();
        reader.getHistory().add(accumulatedLine.toString());

        // use an alternative prompt during accumulating lines
        curPrompt = StringUtils.repeat(" ", prompt.length());
        continue;
      }
    }
    return code;
  }

  private void invokeCommand(String [] cmds) {
    // this command should be moved to GlobalEngine
    TajoShellCommand invoked;
    try {
      invoked = commands.get(cmds[0]);
      invoked.invoke(cmds);
    } catch (Throwable t) {
      sout.println(t.getMessage());
    }
  }

  public int executeStatements(String line) throws Exception {

    // TODO - comment handling and multi line queries should be improved
    // remove comments
    String filtered = line.replaceAll("--[^\\r\\n]*", "").trim();

    String stripped;
    for (String statement : filtered.split(";")) {
      stripped = StringUtils.chomp(statement);
      if (StringUtils.isBlank(stripped)) {
        continue;
      }

      String [] cmds = stripped.split(" ");
      if (cmds[0].equalsIgnoreCase("exit") || cmds[0].equalsIgnoreCase("quit")) {
        sout.println("\n\nbye!");
        sout.flush();
        ((PersistentHistory)this.reader.getHistory()).flush();
        System.exit(0);
      } else if (cmds[0].equalsIgnoreCase("detach") && cmds.length > 1 && cmds[1].equalsIgnoreCase("table")) {
        // this command should be moved to GlobalEngine
        invokeCommand(cmds);

      } else { // submit a query to TajoMaster
        ClientProtos.GetQueryStatusResponse response = client.executeQuery(stripped);
        if (response == null) {
          sout.println("response is null");
        }
        else if (response.getResultCode() == ClientProtos.ResultCode.OK) {
          QueryId queryId = null;
          try {
            queryId = new QueryId(response.getQueryId());
            if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
              sout.println("OK");
            } else {
              getQueryResult(queryId);
            }
          } finally {
            if(queryId != null) {
              client.closeQuery(queryId);
            }
          }
        } else {
          if (response.hasErrorMessage()) {
            sout.println(response.getErrorMessage());
          }
        }
      }
    }
    return 0;
  }

  private boolean isFailed(QueryState state) {
    return state == QueryState.QUERY_ERROR || state == QueryState.QUERY_FAILED;
  }

  private void getQueryResult(QueryId queryId) {
    // if query is empty string
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return;
    }

    // query execute
    try {

      QueryStatus status;
      int initRetries = 0;
      int progressRetries = 0;
      while (true) {
        // TODO - configurabl
        status = client.getQueryStatus(queryId);
        if(status.getState() == QueryState.QUERY_MASTER_INIT || status.getState() == QueryState.QUERY_MASTER_LAUNCHED) {
          Thread.sleep(Math.min(20 * initRetries, 1000));
          initRetries++;
          continue;
        }

        if (status.getState() == QueryState.QUERY_RUNNING ||
            status.getState() == QueryState.QUERY_SUCCEEDED) {
          sout.println("Progress: " + (int)(status.getProgress() * 100.0f)
              + "%, response time: " + ((float)(status.getFinishTime() - status.getSubmitTime()) / 1000.0) + " sec");
          sout.flush();
        }

        if (status.getState() != QueryState.QUERY_RUNNING && status.getState() != QueryState.QUERY_NOT_ASSIGNED) {
          break;
        } else {
          Thread.sleep(Math.min(200 * progressRetries, 1000));
          progressRetries += 2;
        }
      }

      if (status.getState() == QueryState.QUERY_ERROR) {
        sout.println("Internal error!");
      } else if (status.getState() == QueryState.QUERY_FAILED) {
        sout.println("Query failed!");
      } else if (status.getState() == QueryState.QUERY_KILLED) {
        sout.println(queryId + " is killed.");
      } else {
        if (status.getState() == QueryState.QUERY_SUCCEEDED) {
          sout.println("final state: " + status.getState()
              + ", response time: " + (((float)(status.getFinishTime() - status.getSubmitTime()) / 1000.0)
              + " sec"));
          if (status.hasResult()) {
            ResultSet res = null;
            TableDesc desc = null;
            if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
              res = client.createNullResultSet(queryId);
            } else {
              ClientProtos.GetQueryResultResponse response = client.getResultResponse(queryId);
              desc = CatalogUtil.newTableDesc(response.getTableDesc());
              conf.setVar(ConfVars.USERNAME, response.getTajoUserName());
              res = new TajoResultSet(client, queryId, conf, desc);
            }
            try {
              if (res == null) {
                sout.println("OK");
                return;
              }

              ResultSetMetaData rsmd = res.getMetaData();

              TableStats stat = desc.getStats();
              String volume = FileUtil.humanReadableByteCount(stat.getNumBytes(), false);
              long resultRows = stat.getNumRows();
              sout.println("result: " + desc.getPath() + ", " + resultRows + " rows (" + volume + ")");

              int numOfColumns = rsmd.getColumnCount();
              for (int i = 1; i <= numOfColumns; i++) {
                if (i > 1) sout.print(",  ");
                String columnName = rsmd.getColumnName(i);
                sout.print(columnName);
              }
              sout.println("\n-------------------------------");

              int numOfPrintedRows = 0;
              while (res.next()) {
                // TODO - to be improved to print more formatted text
                for (int i = 1; i <= numOfColumns; i++) {
                  if (i > 1) sout.print(",  ");
                  String columnValue = res.getObject(i).toString();
                  if(res.wasNull()){
                    sout.print("null");
                  } else {
                    sout.print(columnValue);
                  }
                }
                sout.println();
                sout.flush();
                numOfPrintedRows++;
                if (numOfPrintedRows >= PRINT_LIMIT) {
                  sout.print("continue... ('q' is quit)");
                  sout.flush();
                  if (sin.read() == 'q') {
                    break;
                  }
                  numOfPrintedRows = 0;
                  sout.println();
                }
              }
            } finally {
              if(res != null) {
                res.close();
              }
            }
          } else {
            sout.println("OK");
          }
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
      System.err.println(t.getMessage());
    }
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "tajo cli [options]", options );
  }

  public int executeCommand(String line) throws Exception {
    String [] metaCommands = line.split(";");
    for (String metaCommand : metaCommands) {
      String arguments [];
      arguments = metaCommand.split(" ");

      TajoShellCommand invoked = commands.get(arguments[0]);
      if (invoked == null) {
        printInvalidCommand(arguments[0]);
        return -1;
      }

      try {
        invoked.invoke(arguments);
      } catch (IllegalArgumentException ige) {
        sout.println(ige.getMessage());
      } catch (Exception e) {
        sout.println(e.getMessage());
      }
    }

    return 0;
  }

  private void printInvalidCommand(String command) {
    sout.println("Invalid command " + command +". Try \\? for help.");
  }

  public static void main(String [] args) throws Exception {
    TajoConf conf = new TajoConf();
    TajoCli shell = new TajoCli(conf, args, System.in, System.out);
    System.out.println();
    int status = shell.runShell();
    System.exit(status);
  }
}
