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
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.tajo.cli.ParsedResult.StatementType.META;
import static org.apache.tajo.cli.ParsedResult.StatementType.STATEMENT;
import static org.apache.tajo.cli.SimpleParser.ParsingState;

public class TajoCli {
  public static final String ERROR_PREFIX = "ERROR: ";
  public static final String KILL_PREFIX = "KILL: ";

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

  private TajoCliOutputFormatter outputFormatter;

  private boolean wasError = false;

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

  protected static final Options options;
  private static final String HOME_DIR = System.getProperty("user.home");
  private static final String HISTORY_FILE = ".tajo_history";

  static {
    options = new Options();
    options.addOption("c", "command", true, "execute only single command, then exit");
    options.addOption("f", "file", true, "execute commands from file, then exit");
    options.addOption("h", "host", true, "Tajo server host");
    options.addOption("p", "port", true, "Tajo server port");
    options.addOption("conf", "conf", true, "configuration value");
    options.addOption("param", "param", true, "parameter value in SQL file");
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

    public void setVariable(String key, String value) {
      conf.set(key, value);
      try {
        initFormatter();
      } catch (Exception e) {
        System.err.println(ERROR_PREFIX + e.getMessage());
      }
    }
  }

  public TajoCli(TajoConf c, String [] args, InputStream in, OutputStream out) throws Exception {
    this.conf = new TajoConf(c);
    this.sin = in;
    this.reader = new ConsoleReader(sin, out);
    this.reader.setExpandEvents(false);
    this.sout = new PrintWriter(reader.getOutput());
    initFormatter();

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

    if (cmd.getOptionValues("conf") != null) {
      for (String eachParam: cmd.getOptionValues("conf")) {
        String[] tokens = eachParam.split("=");
        if (tokens.length != 2) {
          continue;
        }
        conf.set(tokens[0], tokens[1]);
      }
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
      System.err.println(ERROR_PREFIX + "cannot find valid Tajo server address");
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
      outputFormatter.setScirptMode();
      executeScript(cmd.getOptionValue("c"));
      sout.flush();
      System.exit(0);
    }
    if (cmd.hasOption("f")) {
      outputFormatter.setScirptMode();
      cmd.getOptionValues("");
      File sqlFile = new File(cmd.getOptionValue("f"));
      if (sqlFile.exists()) {
        String script = FileUtil.readTextFile(new File(cmd.getOptionValue("f")));
        script = replaceParam(script, cmd.getOptionValues("param"));
        executeScript(script);
        sout.flush();
        System.exit(0);
      } else {
        System.err.println(ERROR_PREFIX + "No such a file \"" + cmd.getOptionValue("f") + "\"");
        System.exit(-1);
      }
    }

    addShutdownHook();
  }

  private void initFormatter() throws Exception {
    Class formatterClass = conf.getClass(ConfVars.CLI_OUTPUT_FORMATTER_CLASS.varname,
        DefaultTajoCliOutputFormatter.class);
    if (outputFormatter == null || !outputFormatter.getClass().equals(formatterClass)) {
      outputFormatter = (TajoCliOutputFormatter)formatterClass.newInstance();
    }
    outputFormatter.init(conf);
  }

  public TajoCliContext getContext() {
    return context;
  }

  protected static String replaceParam(String script, String[] params) {
    if (params == null || params.length == 0) {
      return script;
    }

    for (String eachParam: params) {
      String[] tokens = eachParam.split("=");
      if (tokens.length != 2) {
        continue;
      }
      script = script.replace("${" + tokens[0] + "}", tokens[1]);
    }

    return script;
  }

  private void initHistory() {
    try {
      String historyPath = HOME_DIR + File.separator + HISTORY_FILE;
      if ((new File(HOME_DIR)).exists()) {
        history = new TajoFileHistory(new File(historyPath));
        reader.setHistory(history);
      } else {
        System.err.println(ERROR_PREFIX + "home directory : '" + HOME_DIR +"' does not exist.");
      }
    } catch (Exception e) {
      System.err.println(ERROR_PREFIX + e.getMessage());
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
      wasError = false;
      if (line.startsWith("{")) {
        executeJsonQuery(line);
      } else {
        List<ParsedResult> parsedResults = parser.parseLines(line);

        if (parsedResults.size() > 0) {
          for (ParsedResult parsed : parsedResults) {
            history.addStatement(parsed.getHistoryStatement() + (parsed.getType() == STATEMENT ? ";" : ""));
          }
          executeParsedResults(parsedResults);
          currentPrompt = updatePrompt(parser.getState());
        }
      }
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

      if (wasError && context.getConf().getBoolVar(ConfVars.CLI_STOP_ERROR)) {
        break;
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
        wasError = true;
        return -1;
      }

      try {
        invoked.invoke(arguments);
      } catch (IllegalArgumentException ige) {
        outputFormatter.printErrorMessage(sout, ige);
        wasError = true;
        return -1;
      } catch (Exception e) {
        outputFormatter.printErrorMessage(sout, e);
        wasError = true;
        return -1;
      } finally {
        context.getOutput().flush();
      }

      if (wasError && context.getConf().getBoolVar(ConfVars.CLI_STOP_ERROR)) {
        break;
      }
    }

    return 0;
  }

  private void executeJsonQuery(String json) throws ServiceException {
    long startTime = System.currentTimeMillis();
    ClientProtos.SubmitQueryResponse response = client.executeQueryWithJson(json);
    if (response == null) {
      outputFormatter.printErrorMessage(sout, "response is null");
      wasError = true;
    } else if (response.getResultCode() == ClientProtos.ResultCode.OK) {
      if (response.getIsForwarded()) {
        QueryId queryId = new QueryId(response.getQueryId());
        waitForQueryCompleted(queryId);
      } else {
        if (!response.hasTableDesc() && !response.hasResultSet()) {
          outputFormatter.printMessage(sout, "OK");
          wasError = true;
        } else {
          localQueryCompleted(response, startTime);
        }
      }
    } else {
      if (response.hasErrorMessage()) {
        outputFormatter.printErrorMessage(sout, response.getErrorMessage());
        wasError = true;
      }
    }
  }

  private void executeQuery(String statement) throws ServiceException {
    long startTime = System.currentTimeMillis();
    ClientProtos.SubmitQueryResponse response = client.executeQuery(statement);
    if (response == null) {
      outputFormatter.printErrorMessage(sout, "response is null");
      wasError = true;
    } else if (response.getResultCode() == ClientProtos.ResultCode.OK) {
      if (response.getIsForwarded()) {
        QueryId queryId = new QueryId(response.getQueryId());
        waitForQueryCompleted(queryId);
      } else {
        if (!response.hasTableDesc() && !response.hasResultSet()) {
          outputFormatter.printMessage(sout, "OK");
        } else {
          localQueryCompleted(response, startTime);
        }
      }
    } else {
      if (response.hasErrorMessage()) {
        outputFormatter.printErrorMessage(sout, response.getErrorMessage());
        wasError = true;
      }
    }
  }

  private void localQueryCompleted(ClientProtos.SubmitQueryResponse response, long startTime) {
    ResultSet res = null;
    try {
      QueryId queryId = new QueryId(response.getQueryId());
      float responseTime = ((float)(System.currentTimeMillis() - startTime) / 1000.0f);
      TableDesc desc = new TableDesc(response.getTableDesc());

      // non-forwarded INSERT INTO query does not have any query id.
      // In this case, it just returns succeeded query information without printing the query results.
      if (response.getMaxRowNum() < 0 && queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
        outputFormatter.printResult(sout, sin, desc, responseTime, res);
      } else {
        res = TajoClient.createResultSet(client, response);
        outputFormatter.printResult(sout, sin, desc, responseTime, res);
      }
    } catch (Throwable t) {
      outputFormatter.printErrorMessage(sout, t);
      wasError = true;
    } finally {
      if (res != null) {
        try {
          res.close();
        } catch (SQLException e) {
        }
      }
    }
  }

  private void waitForQueryCompleted(QueryId queryId) {
    // if query is empty string
    if (queryId.equals(QueryIdFactory.NULL_QUERY_ID)) {
      return;
    }

    // query execute
    ResultSet res = null;
    QueryStatus status = null;
    try {

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
          outputFormatter.printProgress(sout, status);
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

      if (status.getState() == QueryState.QUERY_ERROR || status.getState() == QueryState.QUERY_FAILED) {
        outputFormatter.printErrorMessage(sout, status);
        wasError = true;
      } else if (status.getState() == QueryState.QUERY_KILLED) {
        outputFormatter.printKilledMessage(sout, queryId);
        wasError = true;
      } else {
        if (status.getState() == QueryState.QUERY_SUCCEEDED) {
          float responseTime = ((float)(status.getFinishTime() - status.getSubmitTime()) / 1000.0f);
          ClientProtos.GetQueryResultResponse response = client.getResultResponse(queryId);
          if (status.hasResult()) {
            res = TajoClient.createResultSet(client, queryId, response);
            TableDesc desc = new TableDesc(response.getTableDesc());
            outputFormatter.printResult(sout, sin, desc, responseTime, res);
          } else {
            TableDesc desc = new TableDesc(response.getTableDesc());
            outputFormatter.printResult(sout, sin, desc, responseTime, res);
          }
        }
      }
    } catch (Throwable t) {
      outputFormatter.printErrorMessage(sout, t);
      wasError = true;
    } finally {
      if (res != null) {
        try {
          res.close();
        } catch (SQLException e) {
        }
      } else {
        if (status != null && status.getQueryId() != null) {
          client.closeQuery(status.getQueryId());
        }
      }
    }
  }

  public int executeScript(String script) throws Exception {
    wasError = false;
    List<ParsedResult> results = SimpleParser.parseScript(script);
    executeParsedResults(results);
    return 0;
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("tsql [options] [database]", options);
  }

  private void printInvalidCommand(String command) {
    sout.println("Invalid command " + command + ". Try \\? for help.");
  }

  public void close() {
    //for testcase
    if (client != null) {
      client.close();
    }
  }

  public static void main(String [] args) throws Exception {
    TajoConf conf = new TajoConf();
    TajoCli shell = new TajoCli(conf, args, System.in, System.out);
    System.out.println();
    int status = shell.runShell();
    System.exit(status);
  }
}
