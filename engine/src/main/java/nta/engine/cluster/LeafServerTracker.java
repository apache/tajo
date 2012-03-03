/**
 * 
 */
package nta.engine.cluster;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkListener;
import nta.zookeeper.ZkUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

/**
 * @author Hyunsik Choi
 */
public class LeafServerTracker extends ZkListener {
	private final Log LOG = LogFactory.getLog(LeafServerTracker.class); 
	NavigableSet<ServerName> members = new TreeSet<ServerName>();
	private final ZkClient client;
	public static final String LEAF_SERVERS = "/nta/leafservers";
	
	public LeafServerTracker(ZkClient client) throws Exception {
		this.client = client;
	}
	
	public void start() throws Exception {		
		this.client.subscribe(this);
		List<String> servers = ZkUtil.listChildrenAndWatchThem(this.client, LEAF_SERVERS);
		add(servers);
	}
	
	private void add(final List<String> servers) throws IOException {
	    synchronized(this.members) {
	      this.members.clear();
	      for (String n: servers) {
	        ServerName sn = ServerName.create(ZkUtil.getNodeName(n));
	        this.members.add(sn);
	      }
	    }
	  }

	private void remove(ServerName sn) {
		synchronized (members) {
			members.remove(sn);
		}
	}

	@Override
	public void nodeDeleted(String path) {
		if (path.startsWith(LEAF_SERVERS)) {
			String serverName = ZkUtil.getNodeName(path);
			LOG.info("LeafServer ephemeral node deleted, processing expiration [" +
					serverName + "]");
			ServerName sn = ServerName.create(serverName);
			// TODO - node updates		      
			remove(sn);
		}
	}

	@Override
	public void nodeChildrenChanged(String path) {
		if (path.equals("/nta/leafservers")) {
			try {
				List<String> servers =
						ZkUtil.listChildrenAndWatchThem(client, LEAF_SERVERS);
				add(servers);
			} catch (IOException e) {
				LOG.error(e.getMessage(), e);
			} catch (KeeperException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	public List<String> getMembers() {
		try {
			return client.getChildren(LEAF_SERVERS);
		 } catch (KeeperException e) {
		      LOG.warn("Unable to list children of znode " + LEAF_SERVERS + " ", e);	      
		      return null;
		    } catch (InterruptedException e) {
		      LOG.warn("Unable to list children of znode " + LEAF_SERVERS + " ", e);
		      return null;
		    }
	}

	public void close() {
		this.client.close();
	}
}
