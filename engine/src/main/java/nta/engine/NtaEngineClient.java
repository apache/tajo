package nta.engine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Scanner;

import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.conf.NtaConf;
import nta.engine.json.GsonCreator;
import nta.engine.exception.NTAQueryException;
import nta.engine.ipc.QueryEngineInterface;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;

import com.google.gson.Gson;

public class NtaEngineClient {
	
	static String[] history = new String[20];

  /**
   * @param args
   * @throws Exception 
   */
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
		  masterHost = "127.0.1.1";
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
    System.out.print("tazo> ");
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
    		  TableDesc desc = gson.fromJson(json, TableDescImpl.class);
    		  System.out.println(desc);
    	  } else {
    		  System.out.println("Table name is required");
    	  }
      } else if (cmds[0].equalsIgnoreCase("attach")){
        if(cmds.length != 2) {
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
          String tablePath = cli.executeQuery(query);
          FileStatus[] outs = fs.listStatus(new Path(tablePath));
          for (FileStatus out : outs) {
            FSDataInputStream ins = fs.open(out.getPath());
            while (ins.available() > 0) {
              System.out.println(ins.readLine());
            }
          }
        } catch (NTAQueryException nqe) {
          System.err.println(nqe.getMessage());
        }
      }
      System.out.print("tazo> ");
    }
  }
}