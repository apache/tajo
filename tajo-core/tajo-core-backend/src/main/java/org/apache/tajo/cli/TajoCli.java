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
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.util.FileUtil;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TajoCli {
  private final TajoConf conf;
  private static final Options options;

  private TajoClient client;

  private final ConsoleReader reader;
  private final InputStream sin;
  private final PrintWriter sout;

  private static final int PRINT_LIMIT = 24;
  private final Map<String, Command> commands = new TreeMap<String, Command>();

  private static final Class [] registeredCommands = {
      DescTableCommand.class,
      HelpCommand.class,
      DetachCommand.class,
      ExitCommand.class,
      Copyright.class,
      Version.class
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
      Command cmd = null;
      try {
         Constructor cons = clazz.getConstructor(new Class[] {TajoCli.class});
         cmd = (Command) cons.newInstance(this);
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
    String accumulatedLine = "";
    String prompt = "tajo";
    String curPrompt = prompt;
    boolean newStatement = true;
    int code = 0;

    sout.write("Try \\? for help.\n");
    while((raw = reader.readLine(curPrompt + "> ")) != null) {
      // each accumulated line has a space delimiter
      if (!accumulatedLine.equals("")) {
        accumulatedLine += ' ';
      }

      line = raw.trim();

      if (line.length() == 0) { // if empty line
        continue;

      } else if (line.charAt(0) == '/') { // warning for legacy usage
        printInvalidCommand(line);
        continue;

      } else if (line.charAt(0) == '\\') { // command mode
        executeCommand(line);
        ((PersistentHistory)reader.getHistory()).flush();

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
        accumulatedLine = "";
        curPrompt = prompt;

      } else {
        line = StringUtils.chomp(raw).trim();

        // accumulate a line
        accumulatedLine = accumulatedLine + line;

        // replace the latest line with a accumulated line
        if (!newStatement) { // if this is not first line, remove one more line.
          reader.getHistory().removeLast();
        } else {
          newStatement = false;
        }
        reader.getHistory().removeLast();
        reader.getHistory().add(accumulatedLine);

        // use an alternative prompt during accumulating lines
        curPrompt = StringUtils.repeat(" ", prompt.length());
        continue;
      }
    }
    return code;
  }

  private void invokeCommand(String [] cmds) {
    // this command should be moved to GlobalEngine
    Command invoked;
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
        if (response.getResultCode() == ClientProtos.ResultCode.OK) {
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
      while (true) {
        // TODO - configurable
        Thread.sleep(1000);
        status = client.getQueryStatus(queryId);
        if(status.getState() == QueryState.QUERY_MASTER_INIT || status.getState() == QueryState.QUERY_MASTER_LAUNCHED) {
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
        }
      }

      if (isFailed(status.getState())) {
        sout.println(status.getErrorMessage());
      } else if (status.getState() == QueryState.QUERY_KILLED) {
        sout.println(queryId + " is killed.");
      } else {
        if (status.getState() == QueryState.QUERY_SUCCEEDED) {
          sout.println("final state: " + status.getState()
              + ", init time: " + (((float)(status.getInitTime() - status.getSubmitTime()) / 1000.0) + " sec")
              + ", response time: " + (((float)(status.getFinishTime() - status.getSubmitTime()) / 1000.0)
              + " sec"));
          if (status.hasResult()) {
            ResultSet res = client.getQueryResult(queryId);
            try {
              if (res == null) {
                sout.println("OK");
                return;
              }

              ResultSetMetaData rsmd = res.getMetaData();
              TableDesc desc = client.getResultDesc(queryId);
              TableStat stat = desc.getMeta().getStat();
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

  public static abstract class Command {
    public abstract String getCommand();
    public abstract void invoke(String [] command) throws Exception;
    public abstract String getUsage();
    public abstract String getDescription();
  }

  private String toFormattedString(TableDesc desc) {
    StringBuilder sb = new StringBuilder();
    sb.append("\ntable name: ").append(desc.getName()).append("\n");
    sb.append("table path: ").append(desc.getPath()).append("\n");
    sb.append("store type: ").append(desc.getMeta().getStoreType()).append("\n");
    if (desc.getMeta().getStat() != null) {
      sb.append("number of rows: ").append(desc.getMeta().getStat().getNumRows()).append("\n");
      sb.append("volume: ").append(
          FileUtil.humanReadableByteCount(desc.getMeta().getStat().getNumBytes(),
              true)).append("\n");
    }
    sb.append("Options: \n");
    for(Map.Entry<String, String> entry : desc.getMeta().toMap().entrySet()){
      sb.append("\t").append("'").append(entry.getKey()).append("'").append("=")
          .append("'").append(entry.getValue()).append("'").append("\n");
    }
    sb.append("\n");
    sb.append("schema: \n");

    for(int i = 0; i < desc.getMeta().getSchema().getColumnNum(); i++) {
      Column col = desc.getMeta().getSchema().getColumn(i);
      sb.append(col.getColumnName()).append("\t").append(col.getDataType().getType());
      if (col.getDataType().hasLength()) {
        sb.append("(").append(col.getDataType().getLength()).append(")");
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  public class DescTableCommand extends Command {
    public DescTableCommand() {}

    @Override
    public String getCommand() {
      return "\\d";
    }

    @Override
    public void invoke(String[] cmd) throws Exception {
      if (cmd.length == 2) {
        TableDesc desc = client.getTableDesc(cmd[1]);
        if (desc == null) {
          sout.println("Did not find any relation named \"" + cmd[1] + "\"");
        } else {
          sout.println(toFormattedString(desc));
        }
      } else if (cmd.length == 1) {
        List<String> tableList = client.getTableList();
        if (tableList.size() == 0) {
          sout.println("No Relation Found");
        }
        for (String table : tableList) {
          sout.println(table);
        }
      } else {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public String getUsage() {
      return "[table_name]";
    }

    @Override
    public String getDescription() {
      return "show table description";
    }
  }

  public class HelpCommand extends Command {

    @Override
    public String getCommand() {
      return "\\?";
    }

    @Override
    public void invoke(String[] cmd) throws Exception {
      sout.println();

      sout.println("General");
      sout.println("  \\copyright  show Apache License 2.0");
      sout.println("  \\version    show Tajo version");
      sout.println("  \\?          show help");
      sout.println("  \\q          quit tsql");
      sout.println();
      sout.println();

      sout.println("Informational");
      sout.println("  \\d         list tables");
      sout.println("  \\d  NAME   describe table");
      sout.println();
      sout.println();

      sout.println("Documentations");
      sout.println("  tsql guide        http://wiki.apache.org/tajo/tsql");
      sout.println("  Query language    http://wiki.apache.org/tajo/QueryLanguage");
      sout.println("  Functions         http://wiki.apache.org/tajo/Functions");
      sout.println("  Backup & restore  http://wiki.apache.org/tajo/BackupAndRestore");
      sout.println("  Configuration     http://wiki.apache.org/tajo/Configuration");
      sout.println();
    }

    @Override
    public String getUsage() {
      return "";
    }

    @Override
    public String getDescription() {
      return "show command lists and their usages";
    }
  }

  // TODO - This should be dealt as a DDL statement instead of a command
  public class DetachCommand extends Command {
    @Override
    public String getCommand() {
      return "detach";
    }

    @Override
    public void invoke(String[] cmd) throws Exception {
      if (cmd.length != 3) {
        throw new IllegalArgumentException("usage: detach table [tb_name]");
      } else {
        if (client.existTable(cmd[2])) {
          client.detachTable(cmd[2]);
          sout.println("Table \"" + cmd[2] + "\" is detached.");
        } else {
          sout.println("ERROR:  table \"" + cmd[1] + "\" does not exist");
        }
      }
    }

    @Override
    public String getUsage() {
      return "table [table_name]";
    }

    @Override
    public String getDescription() {
      return "detach a table, but it does not remove the table directory.";
    }
  }

  public class Version extends Command {

    @Override
    public String getCommand() {
      return "\\version";
    }

    @Override
    public void invoke(String[] cmd) throws Exception {
      sout.println(TajoConstants.TAJO_VERSION);
    }

    @Override
    public String getUsage() {
      return "";
    }

    @Override
    public String getDescription() {
      return "show Apache License 2.0";
    }
  }

  public class Copyright extends Command {

    @Override
    public String getCommand() {
      return "\\copyright";
    }

    @Override
    public void invoke(String[] cmd) throws Exception {
      sout.println();
      sout.println(
      "  Licensed to the Apache Software Foundation (ASF) under one\n" +
      "  or more contributor license agreements.  See the NOTICE file\n" +
      "  distributed with this work for additional information\n" +
      "  regarding copyright ownership.  The ASF licenses this file\n" +
      "  to you under the Apache License, Version 2.0 (the\n" +
      "  \"License\"); you may not use this file except in compliance\n" +
      "  with the License.  You may obtain a copy of the License at\n" +
      "\n" +
      "       http://www.apache.org/licenses/LICENSE-2.0\n" +
      "\n" +
      "   Unless required by applicable law or agreed to in writing, software\n" +
      "   distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
      "   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
      "   See the License for the specific language governing permissions and\n" +
      "   limitations under the License.");
      sout.println();
    }

    @Override
    public String getUsage() {
      return "";
    }

    @Override
    public String getDescription() {
      return "show Apache License 2.0";
    }
  }

  public class ExitCommand extends Command {

    @Override
    public String getCommand() {
      return "\\q";
    }

    @Override
    public void invoke(String[] cmd) throws Exception {
      sout.println("bye!");
      System.exit(0);
    }

    @Override
    public String getUsage() {
      return "";
    }

    @Override
    public String getDescription() {
      return "quit";
    }
  }

  public int executeCommand(String line) throws Exception {
    String [] metaCommands = line.split(";");
    for (String metaCommand : metaCommands) {
      String arguments [];
      arguments = metaCommand.split(" ");

      Command invoked = commands.get(arguments[0]);
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
