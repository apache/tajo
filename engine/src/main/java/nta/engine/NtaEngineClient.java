package nta.engine;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Scanner;

import nta.catalog.Column;
import nta.catalog.TableDesc;
import nta.conf.NtaConf;
import nta.engine.ipc.QueryEngineInterface;
import nta.engine.json.GsonCreator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ipc.RPC;

import com.google.gson.Gson;

public class NtaEngineClient {
	
	static String[] history = new String[20];

  /**
   * @param args
   * @throws Exception 
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
	  String masterHost = null;
	  int masterPort = -1;
	  
	  for (int i = 0; i < args.length; i++ ) {
		  if (args[i].equals("-host")) {
			  masterHost = args[++i];
		  } else if (args[i].equals("-port")) {
			  masterPort = Integer.valueOf(args[++i]);
		  }
	  }
	  
	  if (masterHost == null) {
		  masterHost = "127.0.0.1";
	  }
	  if (masterPort == -1) {
		  masterPort = 9001;
	  }

    NtaConf conf = new NtaConf();
    FileSystem fs = FileSystem.get(conf);
    QueryEngineInterface cli = 
        (QueryEngineInterface) RPC.getProxy(QueryEngineInterface.class, 
        		QueryEngineInterface.versionId, 
            new InetSocketAddress(masterHost,masterPort), conf);

    Scanner in = new Scanner(System.in);
    String query = null;
    System.out.print("tajo> ");
    while((query = in.nextLine()).compareTo("exit") != 0) {
      
      String [] cmds = query.split(" ");
      
      if (cmds[0].equalsIgnoreCase("cluster")) {
    	  String json = cli.getClusterInfo();
    	  List<String> servers = GsonCreator.getInstance().fromJson(json, List.class);
        for(String server : servers) {
          System.out.println(server);
        }
      } else if (cmds[0].equalsIgnoreCase("\\t")) {
    	  String json = cli.getTableList();
    	  List<String> tables = GsonCreator.getInstance().fromJson(json, List.class);
    	  for (String table : tables) {
    		  System.out.println(table);
    	  }
      } else if (cmds[0].equalsIgnoreCase("\\d")) {
    	  if (cmds.length > 1) {
    		  String json = cli.getTableDesc(cmds[1]);
    		  Gson gson = GsonCreator.getInstance();
    		  TableDesc desc = gson.fromJson(json, TableDesc.class);
    		  System.out.println(toFormattedString(desc));
    	  } else {
    		  System.out.println("Table name is required");
    	  }
      } else if (cmds[0].equalsIgnoreCase("attach")){
        if(cmds.length != 3) {
          System.out.println("usage: attach tablename path");
        }
        cli.attachTable(cmds[1], cmds[2]);
      } else if (cmds[0].equalsIgnoreCase("detach")) {
    	  if (cmds.length != 2) {
    		  System.out.println("usage: detach tablename");
    	  }
      } else {
        // query execute
        try {
          long start = System.currentTimeMillis();
          String tablePath = cli.executeQuery(query);
          long end = System.currentTimeMillis();
          System.out.println("write the result into " + tablePath 
              + " (" + (end - start) +"msc)");
          
        // TODO - the result should be printed.
        } catch (Throwable t) {
          System.err.println(t.getMessage());
        }
      }
      System.out.print("tajo> ");
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
}