/**
 * 
 */
package tajo.cli;

import jline.ConsoleReader;
import jline.History;
import nta.catalog.Column;
import nta.catalog.TableDesc;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.engine.cluster.ServerName;
import nta.engine.cluster.ServerNodeTracker;
import nta.zookeeper.ZkClient;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import tajo.client.TajoClient;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

/**
 * @author Hyunsik Choi
 */
public class TajoCli {
  private final Configuration conf;
  private static final Options options;
  private ZkClient zkClient;
  private ServerNodeTracker masterTracker;
  private TajoClient client;
  
  private String zkAddr;
  private String entryAddr;  
  private static final int WAIT_TIME = 3000;
  private static final int PRINT_LIMIT = 24;
  
  private ConsoleReader reader;
  private InputStream sin;
  private PrintWriter sout;
  
  static {
    options = new Options();
    options.addOption("a", "addr", true, "client service address (hostname:port)");
    options.addOption("conf", true, "user configuration dir");
    options.addOption("h", "help", false, "help");
  }
  
  public TajoCli(Configuration c, String [] args, 
      InputStream in, Writer out) throws Exception {
    this.conf = new Configuration(c);
    this.sin = in;
    this.sout = new PrintWriter(out);
    
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
          NConstants.DEFAULT_CLIENT_SERVICE_PORT);

      this.entryAddr = sn.getServerName();
      conf.set(NConstants.CLIENT_SERVICE_ADDRESS, this.entryAddr);
    }

    // if there is no "-a" option,
    if(this.entryAddr == null) {
      // it checks if the '-z' option is given
      if (cmd.hasOption("z")) {
        zkAddr = cmd.getOptionValue("z");
        zkClient = new ZkClient(zkAddr);
        masterTracker = new ServerNodeTracker(zkClient, NConstants.ZNODE_CLIENTSERVICE);
        masterTracker.start();
        byte [] entryAddrBytes;
        do {
          entryAddrBytes = masterTracker.blockUntilAvailable(WAIT_TIME);
          sout.println("Waiting the zookeeper (" + zkAddr + ")");
        } while (entryAddrBytes == null);

        this.entryAddr = new String(entryAddrBytes);
        conf.set(NConstants.ZOOKEEPER_ADDRESS, this.zkAddr);
        conf.set(NConstants.CLIENT_SERVICE_ADDRESS, this.entryAddr);
      } else if (conf.get(NConstants.CLIENT_SERVICE_ADDRESS) != null &&
          !conf.get(NConstants.CLUSTER_DISTRIBUTED).equals(NConstants.CLUSTER_IS_LOCAL)) {
        // it checks if the client service address is given in configuration and distributed mode.
        // if so, it sets entryAddr.
        entryAddr = conf.get(NConstants.CLIENT_SERVICE_ADDRESS);
      }
    }

    // if the remote tajo cluster is set, entryAddr is not null.
    if (entryAddr != null) {
      conf.set(NConstants.CLUSTER_DISTRIBUTED, "true");
    }

    if (entryAddr != null) {
      sout.println("Trying to connect the tajo master (" + entryAddr + ")");
    } else {
      sout.println("Executing the tajo cluster in local mode");
    }
    client = new TajoClient(conf);
  }
  
  public int executeShell() throws Exception {
    reader = new ConsoleReader(sin, sout);
    
    String line;
    String cmd [];
    boolean quit = false;
    while(!quit) {
      line = reader.readLine("tajo> ");
      if (line == null) { // if EOF, quit
        quit = true;
        continue;
      }
      
      cmd = line.split(" ");
      
      if (cmd[0].equalsIgnoreCase("exit") || cmd[0].equalsIgnoreCase("quit")) {        
        quit = true;
      } else if (cmd[0].equalsIgnoreCase("\\c")) {
        clusterInfo();
      } else if (cmd[0].equalsIgnoreCase("\\t")) {
        showTables();
      } else if (cmd[0].equalsIgnoreCase("\\d")) {
        descTable(cmd);
      } else if (cmd[0].equalsIgnoreCase("attach")) {
        attachTable(cmd);
      } else if (cmd[0].equalsIgnoreCase("detach")) {
        detachTable(cmd);
      } else if (cmd[0].equalsIgnoreCase("history")) {
        showHistory();
      } else {
       executeQuery(line); 
      }
    }
    
    sout.println("\n\nbye from data deluge...");
    sout.flush();
    return 0;
  }
  
  private void executeQuery(String queryStr) {
    // if query is empty string
    if (queryStr.equals("")) {
      return;
    }

    // query execute
    try {
      ResultSet res = client.executeQuery(queryStr);
      ResultSetMetaData rsmd = res.getMetaData();
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
      System.err.println(t.getMessage());
    }
  }
  
  private void clusterInfo() {
    List<String> list = client.getClusterInfo();    
    for(String server : list) {
      sout.println(server);
    }
  }
  
  private void showTables() {
    List<String> tableList = client.getTableList();
    for (String table : tableList) {
      sout.println(table);
    }
  }
  
  private void descTable(String [] cmd) {
    if (cmd.length > 1) {
      TableDesc desc = client.getTableDesc(cmd[1]);
      sout.println(toFormattedString(desc));
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
  
  @SuppressWarnings("unchecked")
  private void showHistory() {
    History history = reader.getHistory();
    int i = 0;
    for (String cmd : (List<String>)history.getHistoryList()) {
      sout.println(i +": " +cmd);
    }
  }
  
  private static String toFormattedString(TableDesc desc) {
    StringBuilder sb = new StringBuilder();
    sb.append("\ntable name: ").append(desc.getId()).append("\n");
    sb.append("table path: ").append(desc.getPath()).append("\n");
    sb.append("store type: ").append(desc.getMeta().getStoreType()).append("\n");
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
    Configuration conf = NtaConf.create();
    PrintWriter out = new PrintWriter(System.out);
    TajoCli shell = new TajoCli(conf, args, 
        System.in, out);
    System.out.println();
    int status = shell.executeShell();
    System.exit(status);
  }
}
