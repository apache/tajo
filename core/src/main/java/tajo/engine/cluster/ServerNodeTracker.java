package tajo.engine.cluster;

import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkNodeTracker;

/**
 * This class is an utility class for a server mapping to
 * a specific znode in zookeeper.
 * 
 * 
 * @author Hyunsik Choi
 * 
 */
public class ServerNodeTracker extends ZkNodeTracker {

  public ServerNodeTracker(ZkClient client, String znodePath) {
    super(client, znodePath);
  }

  public ServerName getServerAddress() {
    byte[] data = super.getData();
    return data == null ? null : new ServerName(new String(data));
  }

  public synchronized ServerName waitForServer(long timeout)
      throws InterruptedException {
    byte[] data = super.blockUntilAvailable();
    return data == null ? null : new ServerName(new String(data));
  }
}
