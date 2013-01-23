/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.cli;

import com.google.protobuf.ServiceException;
import jline.console.ConsoleReader;
import org.apache.commons.cli.*;
import tajo.QueryId;
import tajo.catalog.Column;
import tajo.catalog.TableDesc;
import tajo.client.ClientProtocol;
import tajo.client.QueryStatus;
import tajo.client.TajoClient;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.master.cluster.ServerName;
import tajo.util.FileUtil;
import tajo.util.TajoIdUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

/**
 * @author Hyunsik Choi
 */
public class TajoCli {
  private final TajoConf conf;
  private static final Options options;
  private TajoClient client;

  private String entryAddr;
  private static final int PRINT_LIMIT = 24;

  private ConsoleReader reader;
  private InputStream sin;
  private OutputStream out;
  private PrintWriter sout;


  static {
    options = new Options();
    options.addOption("a", "addr", true, "client service address (hostname:port)");
    options.addOption("conf", true, "user configuration dir");
    options.addOption("h", "help", false, "help");
  }

  public TajoCli(TajoConf c, String [] args,
                 InputStream in, OutputStream out) throws Exception {
    this.conf = new TajoConf(c);
    this.sin = in;
    this.out = out;
    reader = new ConsoleReader(sin, out);
    this.sout = new PrintWriter(reader.getOutput());

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("h")) {
      printUsage();
      System.exit(-1);
    }

    // firstly, the specific address (option -a) is used
    String addr;
    if (entryAddr == null && cmd.hasOption("a")) {
      addr = cmd.getOptionValue("a");
      ServerName sn = ServerName.createWithDefaultPort(addr,
          conf.getIntVar(TajoConf.ConfVars.CLIENT_SERVICE_PORT));

      this.entryAddr = sn.getServerName();
      conf.setVar(TajoConf.ConfVars.CLIENT_SERVICE_ADDRESS, this.entryAddr);
    }

    // if there is no "-a" option,
    if(this.entryAddr == null) {
      if (conf.getVar(ConfVars.CLIENT_SERVICE_ADDRESS) != null &&
          conf.getBoolVar(ConfVars.CLUSTER_DISTRIBUTED)) {
        // it checks if the client service address is given in configuration and distributed mode.
        // if so, it sets entryAddr.
        entryAddr = conf.getVar(ConfVars.CLIENT_SERVICE_ADDRESS);
      }
    }

    // if the remote tajo cluster is set, entryAddr is not null.
    if (entryAddr != null) {
      conf.setBoolVar(ConfVars.CLUSTER_DISTRIBUTED, true);
    }

    if (entryAddr != null) {
      sout.println("Trying to connect the tajo master (" + entryAddr + ")");
    } else {
      sout.println("Executing the tajo cluster in local mode");
    }
    client = new TajoClient(conf);
  }

  public int executeShell() throws Exception {

    String line;
    String cmd [];
    boolean quit = false;
    while(!quit) {
      line = reader.readLine("tajo> ");
      if (line == null) { // if EOF, quit
        quit = true;
        continue;
      } else if (line.length() == 0) {
        continue;
      }

      cmd = line.split(" ");

      if (cmd[0].equalsIgnoreCase("exit") || cmd[0].equalsIgnoreCase("quit")) {
        quit = true;
      } else if (cmd[0].equalsIgnoreCase("/c")) {
        clusterInfo();
      } else if (cmd[0].equalsIgnoreCase("/t")) {
        showTables();
      } else if (cmd[0].equalsIgnoreCase("/d")) {
        descTable(cmd);
      } else if (cmd[0].equalsIgnoreCase("attach")) {
        attachTable(cmd);
      } else if (cmd[0].equalsIgnoreCase("detach")) {
        detachTable(cmd);
      } else if (cmd[0].equalsIgnoreCase("history")) {

      } else {
        ClientProtocol.SubmitQueryRespose response = client.executeQuery(line);

        if (response.getResultCode() == ClientProtocol.ResultCode.OK) {
          QueryId queryId = new QueryId(response.getQueryId());
          if (queryId.equals(TajoIdUtils.NullQueryId)) {
            sout.println("OK");
          } else {
            getQueryResult(queryId);
          }
        } else {
        if (response.hasErrorMessage()) {
          sout.println(response.getErrorMessage());
        }
        }
      }
    }

    sout.println("\n\nbye from data deluge...");
    sout.flush();
    return 0;
  }

  private void getQueryResult(QueryId queryId) {
    // if query is empty string
    if (queryId.equals(TajoIdUtils.NullQueryId)) {
      return;
    }

    // query execute
    try {
      ResultSet res = client.getQueryResultAndWait(queryId);
      if (res == null) {
        sout.println("OK");
        return;
      }
      QueryStatus status = client.getQueryStatus(queryId);
      ResultSetMetaData rsmd = res.getMetaData();
      TableDesc desc = client.getResultDesc(queryId);
      sout.println("final state: " + status.getState()
          + ", init time: " + (((double)(status.getInitTime() - status.getSubmitTime()) / 1000.0) + " sec")
          + ", execution time: " + (((double)status.getFinishTime() - status.getInitTime()) / 1000.0) + " sec"
          + ", total response time: " + (((double)(status.getFinishTime() - status.getSubmitTime()) / 1000.0) + " sec"));
      sout.println("result: " + desc.getPath() + "\n");

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
          sout.print(columnValue);
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
    } catch (Throwable t) {
      t.printStackTrace();
      System.err.println(t.getMessage());
    }
  }

  private void clusterInfo() {
    List<String> list = client.getClusterInfo();
    for(String server : list) {
      sout.println(server);
    }
  }

  private void showTables() throws ServiceException {
    List<String> tableList = client.getTableList();
    for (String table : tableList) {
      sout.println(table);
    }
  }

  private void descTable(String [] cmd) throws ServiceException {
    if (cmd.length > 1) {
      TableDesc desc = client.getTableDesc(cmd[1]);
      if (desc == null) {
        sout.println("No such a table name: " + cmd[1]);
      } else {
        sout.println(toFormattedString(desc));
      }
    } else {
      sout.println("Table name is required");
    }
  }

  private void attachTable(String [] cmd) throws Exception {
    if(cmd.length != 3) {
      sout.println("usage: attach tablename path");
    } else {
      client.attachTable(cmd[1], cmd[2]);
      sout.println("attached " + cmd[1] + " (" + cmd[2] + ")");
    }
  }

  private void detachTable(String [] cmd) throws Exception {
    if (cmd.length != 2) {
      System.out.println("usage: detach tablename");
    } else {
      client.detachTable(cmd[1]);
      sout.println("detached " + cmd[1] + " from tajo");
    }
  }

  private static String toFormattedString(TableDesc desc) {
    StringBuilder sb = new StringBuilder();
    sb.append("\ntable name: ").append(desc.getId()).append("\n");
    sb.append("table path: ").append(desc.getPath()).append("\n");
    sb.append("store type: ").append(desc.getMeta().getStoreType()).append("\n");
    if (desc.getMeta().getStat() != null) {
      sb.append("number of rows: ").append(desc.getMeta().getStat().getNumRows()).append("\n");
      sb.append("volume (bytes): ").append(
          FileUtil.humanReadableByteCount(desc.getMeta().getStat().getNumBytes(),
              true)).append("\n");
    }
    sb.append("schema: \n");

    for(int i = 0; i < desc.getMeta().getSchema().getColumnNum(); i++) {
      Column col = desc.getMeta().getSchema().getColumn(i);
      sb.append(col.getColumnName()).append("\t").append(col.getDataType());
      sb.append("\n");
    }
    return sb.toString();
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "tajo shell [options]", options );
  }

  public static void main(String [] args) throws Exception {
    TajoConf conf = new TajoConf();
    TajoCli shell = new TajoCli(conf, args, System.in, System.out);
    System.out.println();
    int status = shell.executeShell();
    System.exit(status);
  }
}
