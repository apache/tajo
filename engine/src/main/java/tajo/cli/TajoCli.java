/**
 * 
 */
package tajo.cli;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.List;

import jline.ConsoleReader;
import jline.History;
import nta.catalog.CatalogClient;
import nta.catalog.Column;
import nta.catalog.TableDesc;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.engine.cluster.ServerNodeTracker;
import nta.engine.ipc.QueryEngineInterface;
import nta.engine.json.GsonCreator;
import nta.zookeeper.ZkClient;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;

import com.google.gson.Gson;

/**
 * @author Hyunsik Choi
 *
 */
public class TajoCli {
  private final Configuration conf;
  private static final Options options;
  private ZkClient zkClient;
  private ServerNodeTracker masterTracker;
  private QueryEngineInterface proxy;
  @SuppressWarnings("unused")
  private CatalogClient client;
  
  private String zkAddr;
  private String masterAddr;  
  private static final int WAIT_TIME = 3000;
  
  private ConsoleReader reader;
  private InputStream sin;
  private PrintWriter sout;
  
  static {
    options = new Options();
    options.addOption("h", "host", true, "client service host");
    options.addOption("p", "port", true, "client service port");
    options.addOption("z", "zkaddr", true, "zookeeper address");
    options.addOption("conf", true, "user configuration dir");
  }
  
  public TajoCli(Configuration c, String [] args, 
      InputStream in, Writer out) throws Exception {
    this.conf = c;
    this.sin = in;
    this.sout = new PrintWriter(out);
    
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    
    String hostName;
    int givenPort;
    if (masterAddr == null && cmd.hasOption("h")) {
      hostName = cmd.getOptionValue("h");
      if (cmd.hasOption("p")) {
        givenPort = Integer.valueOf(cmd.getOptionValue("p"));
      } else {
        givenPort = NConstants.DEFAULT_MASTER_PORT;
      }
      
      this.masterAddr = hostName + ":" + givenPort;
    }
    
    if(this.masterAddr == null && cmd.hasOption("z")) {
      zkAddr = cmd.getOptionValue("z");
      zkClient = new ZkClient(zkAddr);
      masterTracker = new ServerNodeTracker(zkClient, NConstants.ZNODE_CLIENTSERVICE);
      masterTracker.start();      
      byte [] masterAddrBytes = null; 
      do {
        masterAddrBytes = masterTracker.blockUntilAvailable(WAIT_TIME);
        System.out.println("Waiting the zookeeper (" + zkAddr + ")");
      } while (masterAddrBytes == null);
      
      masterAddr = new String(masterAddrBytes);
    }
    
    if (this.masterAddr == null) {
      printUsage();
      System.exit(-1);
    }
    
    sout.println("Trying to connect the tajo master (" + this.masterAddr + ")");
    proxy = 
        (QueryEngineInterface) RPC.getProxy(QueryEngineInterface.class, 
            QueryEngineInterface.versionId, 
            NetUtils.createSocketAddr(this.masterAddr), conf);
    sout.println("Connected to tajo master (" + this.masterAddr + ")");
  }
  
  public int executeShell() throws Exception {
    reader = new ConsoleReader(sin, sout);
    
    String line = "";
    String cmd [] = null;
    boolean quit = false;
    while(quit == false) {
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
    // query execute
    try {
      long start = System.currentTimeMillis();
      String tablePath = proxy.executeQuery(queryStr);
      long end = System.currentTimeMillis();
      System.out.println("write the result into " + tablePath 
          + " (" + (end - start) +"msc)");
      
    // TODO - the result should be printed.
    } catch (Throwable t) {
      System.err.println(t.getMessage());
    }
  }
  
  @SuppressWarnings("unchecked")
  private void clusterInfo() throws KeeperException, InterruptedException {
    String json = proxy.getClusterInfo();
    List<String> servers = GsonCreator.getInstance().fromJson(json, List.class);
    for(String server : servers) {
      sout.println(server);
    }
  }
  
  private void showTables() {
    String json = proxy.getTableList();
    @SuppressWarnings("unchecked")
    List<String> tables = GsonCreator.getInstance().fromJson(json, List.class);
    for (String table : tables) {
      sout.println(table);
    }
  }
  
  private void descTable(String [] cmd) {
    if (cmd.length > 1) {
      String json = proxy.getTableDesc(cmd[1]);
      Gson gson = GsonCreator.getInstance();
      TableDesc desc = gson.fromJson(json, TableDesc.class);
      sout.println(toFormattedString(desc));
    } else {
      sout.println("Table name is required");
    }
  }
  
  private void attachTable(String [] cmd) throws Exception {
    if(cmd.length != 3) {
      sout.println("usage: attach tablename path");
    } else {
      proxy.attachTable(cmd[1], cmd[2]);
      sout.println("attached " + cmd[1] + " (" + cmd[2] + ")");
    }
  }
  
  private void detachTable(String [] cmd) throws Exception {
    if (cmd.length != 2) {
      System.out.println("usage: detach tablename");
    } else {
      proxy.detachTable(cmd[1]);
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
    sb.append("\ntable name: " + desc.getId()).append("\n");
    sb.append("table path: " + desc.getPath()).append("\n");
    sb.append("store type: " + desc.getMeta().getStoreType()).append("\n");
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
    TajoCli shell = new TajoCli(conf, args, 
        System.in, new PrintWriter(System.out));
    int status = shell.executeShell();
    System.exit(status);
  }
}
