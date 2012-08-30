/**
 * 
 */
package tajo.engine.cluster;

import org.apache.zookeeper.KeeperException;
import tajo.conf.TajoConf;
import tajo.NConstants;
import tajo.zookeeper.ZkClient;

import java.io.IOException;
import java.util.List;

/**
 * @author Hyunsik Choi
 *
 */
public class ClusterUtil {
  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    if (args.length < 1) {
      System.out.println("usage: cluster [list,catalog,master]");      
      System.exit(-1);
    }
    
    ZkClient zkClient = new ZkClient(new TajoConf());
    
    if(args[0].equalsIgnoreCase("list")) {
      List<String> list = zkClient.getChildren(NConstants.ZNODE_LEAFSERVERS);
      for (String server : list) {
        System.out.println(server);
      }
    } else if (args[0].equalsIgnoreCase("catalog")) {
      byte [] server = zkClient.getData(NConstants.ZNODE_CATALOG, null, null);
      System.out.println(new String(server));
    } else if (args[0].equalsIgnoreCase("master")) {
      byte [] server = zkClient.getData(NConstants.ZNODE_MASTER, null, null);
      System.out.println(new String(server));
    }
  }
}