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

import com.google.protobuf.ServiceException;
import jline.console.ConsoleReader;
import org.apache.commons.cli.*;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.util.FileUtil;

import java.io.*;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.tajo.cli.ParsedResult.StatementType.META;
import static org.apache.tajo.cli.ParsedResult.StatementType.STATEMENT;
import static org.apache.tajo.cli.SimpleParser.ParsingState;

public class TajoCli {

  private final TajoConf conf;
  private TajoClient client;
  private final TajoCliContext context;

  // Jline and Console related things
  private final ConsoleReader reader;
  private final InputStream sin;
  private final PrintWriter sout;
  private TajoFileHistory history;

  // Current States
  private String currentDatabase;

  private static final Class [] registeredCommands = {
      DescTableCommand.class,
      DescFunctionCommand.class,
      HelpCommand.class,
      ExitCommand.class,
      CopyrightCommand.class,
      VersionCommand.class,
      ConnectDatabaseCommand.class,
      ListDatabaseCommand.class,
      SetCommand.class,
      UnsetCommand.class,
      ExecExternalShellCommand.class,
      HdfsCommand.class,
      TajoAdminCommand.class
  };
  private final Map<String, TajoShellCommand> commands = new TreeMap<String, TajoShellCommand>();

  public static final int PRINT_LIMIT = 24;
  private static final Options options;
  private static final String HOME_DIR = System.getProperty("user.home");
  private static final String HISTORY_FILE = ".tajo_history";

  static {
    options = new Options();
    options.addOption("c", "command", true, "execute only single command, then exit");
    options.addOption("f", "file", true, "execute commands from file, then exit");
    options.addOption("h", "host", true, "Tajo server host");
    options.addOption("p", "port", true, "Tajo server port");
    options.addOption("help", "help", false, "help");
  }

  public class TajoCliContext {

    public TajoClient getTajoClient() {
      return client;
    }

    public void setCurrentDatabase(String databasae) {
      currentDatabase = databasae;
    }

    public String getCurrentDatabase() {
      return currentDatabase;
    }

    public PrintWriter getOutput() {
      return sout;
    }

    public TajoConf getConf() {
      return conf;
    }
  }

  public TajoCli(TajoConf c, String [] args, InputStream in, OutputStream out) throws Exception {
    this.conf = new TajoConf(c);
    this.sin = in;
    this.reader = new ConsoleReader(sin, out);
    this.reader.setExpandEvents(false);
    this.sout = new PrintWriter(reader.getOutput());

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("help")) {
      printUsage();
    }

    String hostName = null;
    Integer port = null;
    if (cmd.hasOption("h")) {
      hostName = cmd.getOptionValue("h");
    }
    if (cmd.hasOption("p")) {
      port = Integer.parseInt(cmd.getOptionValue("p"));
    }

    String baseDatabase = null;
    if (cmd.getArgList().size() > 0) {
      baseDatabase = (String) cmd.getArgList().get(0);
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
      throw new RuntimeException("cannot find valid Tajo server address");
    } else if (hostName != null && port != null) {
      conf.setVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, hostName+":"+port);
      client = new TajoClient(conf, baseDatabase);
    } else if (hostName == null && port == null) {
      client = new TajoClient(conf, baseDatabase);
    }

    context = new TajoCliContext();
    context.setCurrentDatabase(client.getCurrentDatabase());
    initHistory();
    initCommands();

    if (cmd.hasOption("c")) {
      executeScript(cmd.getOptionValue("c"));
      sout.flush();
      System.exit(0);
    }
    if (cmd.hasOption("f")) {
      File sqlFile = new File(cmd.getOptionValue("f"));
      if (sqlFile.exists()) {
        String script = FileUtil.readTextFile(new File(cmd.getOptionValue("f")));
        executeScript(script);
        sout.flush();
        System.exit(0);
      } else {
        System.err.println("No such a file \"" + cmd.getOptionValue("f") + "\"");
        System.exit(-1);
      }
    }

    addShutdownHook();
  }

  private void initHistory() {
    try {
      String historyPath = HOME_DIR + File.separator + HISTORY_FILE;
      if ((new File(HOME_DIR)).exists()) {
        history = new TajoFileHistory(new File(historyPath));
        reader.setHistory(history);
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
         Constructor cons = clazz.getConstructor(new Class[] {TajoCliContext.class});
         cmd = (TajoShellCommand) cons.newInstance(context);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        throw new RuntimeException(e.getMessage());
      }
      commands.put(cmd.getCommand(), cmd);
    }
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          history.flush();
        } catch (IOException e) {
        }
        client.close();
      }
    }));
  }

  private String updatePrompt(ParsingState state) throws ServiceException {
    if (state == ParsingState.WITHIN_QUOTE) {
      return "'";
    } else if (state == ParsingState.TOK_START) {
      return context.getCurrentDatabase();
    } else {
      return "";
    }
  }

  public int runShell() throws Exception {
    String line;
    String currentPrompt = context.getCurrentDatabase();
    int code = 0;

    sout.write("Try \\? for help.\n");

    SimpleParser parser = new SimpleParser();
    while((line = reader.readLine(currentPrompt + "> ")) != null) {
      if (line.equals("")) {
        continue;
      }

      List<ParsedResult> parsedResults = parser.parseLines(line);

      if (parsedResults.size() > 0) {
        for (ParsedResult parsed : parsedResults) {
          history.addStatement(parsed.getStatement() + (parsed.getType() == STATEMENT ? ";":""));
        }
      }
      executeParsedResults(parsedResults);
      currentPrompt = updatePrompt(parser.getState());
    }
    return code;
  }

  private void executeParsedResults(Collection<ParsedResult> parsedResults) throws Exception {
    for (ParsedResult parsedResult : parsedResults) {
      if (parsedResult.getType() == META) {
        executeMetaCommand(parsedResult.getStatement());
      } else {
        executeQuery(parsedResult.getStatement());
      }
    }
  }

  public int executeMetaCommand(String line) throws Exception {
    String [] metaCommands = line.split(";");
    for (String metaCommand : metaCommands) {
      String arguments [] = metaCommand.split(" ");

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

  private void executeQuery(String statement) throws ServiceException {
    ClientProtos.SubmitQueryResponse response = client.executeQuery(statement);
    if (response == null) {
      sout.println("response is null");
    } else if (response.getResultCode() == ClientProtos.ResultCode.OK) {
      if (response.getIsForwarded()) {
        QueryId queryId = new QueryId(response.getQueryId());
        try {
          waitForQueryCompleted(queryId);
        } finally {
          client.closeQuery(queryId);
        }
      } else {
        if (!response.hasTableDesc() && !response.hasResultSet()) {
          sout.println("Ok");
        } else {

          ResultSet resultSet;
          int numBytes;
          long maxRowNum;
          try {
            resultSet = TajoClient.createResultSet(client, response);
            if (response.hasTableDesc()) {
              numBytes = 0;
            } else {
              numBytes = response.getResultSet().getBytesNum();
            }
            maxRowNum = response.getMaxRowNum();
            printResult(resultSet, maxRowNum, numBytes);
          } catch (IOException ioe) {
            sout.println(ioe.getMessage());
          } catch (SQLException sqe) {
            sout.println(sqe.getMessage());
          }
        }
      }
    } else {
      if (response.hasErrorMessage()) {
        sout.println(response.getErrorMessage());
      }
    }
  }

  private void waitForQueryCompleted(QueryId queryId) {
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
        // TODO - configurable
        status = client.getQueryStatus(queryId);
        if(status.getState() == QueryState.QUERY_MASTER_INIT || status.getState() == QueryState.QUERY_MASTER_LAUNCHED) {
          Thread.sleep(Math.min(20 * initRetries, 1000));
          initRetries++;
          continue;
        }

        if (status.getState() == QueryState.QUERY_RUNNING || status.getState() == QueryState.QUERY_SUCCEEDED) {
          sout.println("Progress: " + (int)(status.getProgress() * 100.0f)
              + "%, response time: " + ((float)(status.getFinishTime() - status.getSubmitTime()) / 1000.0) + " sec");
          sout.flush();
        }

        if (status.getState() != QueryState.QUERY_RUNNING &&
            status.getState() != QueryState.QUERY_NOT_ASSIGNED &&
            status.getState() != QueryState.QUERY_KILL_WAIT) {
          break;
        } else {
          Thread.sleep(Math.min(200 * progressRetries, 1000));
          progressRetries += 2;
        }
      }

      if (status.getState() == QueryState.QUERY_ERROR) {
        sout.println("Internal error!");
        if(status.getErrorMessage() != null && !status.getErrorMessage().isEmpty()) {
          sout.println(status.getErrorMessage());
        }
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
            ClientProtos.GetQueryResultResponse response = client.getResultResponse(queryId);
            ResultSet res = TajoClient.createResultSet(client, queryId, response);
            TableDesc desc = new TableDesc(response.getTableDesc());
            long totalRowNum = desc.getStats().getNumRows();
            long totalBytes = desc.getStats().getNumBytes();
            printResult(res, totalRowNum, totalBytes);
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

  private void printResult(ResultSet res, long rowNum, long numBytes) throws IOException, SQLException  {
    try {
      if (res == null) {
        sout.println("OK");
        return;
      }

      ResultSetMetaData rsmd = res.getMetaData();

      String volume = FileUtil.humanReadableByteCount(numBytes, false);
      String rowNumStr = rowNum == Integer.MAX_VALUE ? "unknown" : rowNum + "";
      sout.println("result: " + rowNumStr + " rows (" + volume + ")");

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
            sout.println();
            break;
          }
          numOfPrintedRows = 0;
          sout.println();
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if(res != null) {
        res.close();
      }
    }
  }

  public int executeScript(String script) throws Exception {
    List<ParsedResult> results = SimpleParser.parseScript(script);
    executeParsedResults(results);
    return 0;
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "tsql [options] [database]", options );
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
