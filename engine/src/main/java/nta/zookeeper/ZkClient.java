package nta.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import nta.engine.NConstants;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkClient implements Watcher {
	private final Log LOG = LogFactory.getLog(ZkClient.class);	

	private CountDownLatch latch = new CountDownLatch(1);

	private ZooKeeper zk;
	int sessionTimeout = 3000;

	private final List<ZkListener> listeners = new CopyOnWriteArrayList<ZkListener>();	

	public ZkClient(Configuration conf) throws IOException {
		this(conf.get(NConstants.ZOOKEEPER_HOST)+":"+
			conf.getInt(NConstants.ZOOKEEPER_PORT, 
				NConstants.DEFAULT_ZOOKEEPER_PORT),
				conf.getInt(NConstants.ZOOKEEPER_SESSION_TIMEOUT, 
					NConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT));
	}

	public ZkClient(String serverstring) throws IOException {
		this(serverstring, NConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
	}

	public ZkClient(String serverstring, int sessionTimeout) throws IOException {
		this.zk = new ZooKeeper(serverstring, sessionTimeout, this);
		try {
			this.latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void subscribe(ZkListener listener) {
		this.listeners.add(listener);		
	}

	public void unsubscribe(ZkListener listener) {
		this.listeners.remove(listener);
	}

	public void createPersistent(String path) throws KeeperException, InterruptedException {
		zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public void createPersistent(final String path, final byte [] data) 
		throws KeeperException, InterruptedException  {
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public void createEphemeral(final String path) throws KeeperException, InterruptedException {
		zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	public void createEphemeral(final String path, final byte [] data) 
		throws KeeperException, InterruptedException {
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	public String createEphemeralSequential(final String path, final byte [] data) 
		throws Exception {
		return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	public Stat exists(final String path) throws KeeperException, InterruptedException {
		return zk.exists(path, true);
	}

	public Stat exists(final String path, boolean watch) throws KeeperException, InterruptedException {
		return zk.exists(path, watch);
	}

	public void delete(final String path) throws Exception {
		zk.delete(path, -1);
	}

	@Override
	public void process(WatchedEvent event) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received Zookeeper Event, " +
				"type=" + event.getType() + ", " +
				"state=" + event.getState() + ", " +
				"path=" + event.getPath());
		}

		switch(event.getType()) {
		case None: {
			if(event.getState() == KeeperState.SyncConnected) {
				latch.countDown();
			}
		}

		case NodeCreated: {
			for(ZkListener listener : this.listeners) {
				listener.nodeCreated(event.getPath());
			}
			break;
		}		

		case NodeDeleted: {
			for(ZkListener listener : this.listeners) {
				listener.nodeDeleted(event.getPath());
			}
			break;
		}

		case NodeDataChanged: {
			for(ZkListener listener : this.listeners) {
				listener.nodeDataChanged(event.getPath());
			}
			break;
		}

		case NodeChildrenChanged: {
			for(ZkListener listener : this.listeners) {
				listener.nodeChildrenChanged(event.getPath());
			}
			break;
		}
		}
	}

	public byte[] getData(String path, Watcher watcher, Stat stat)
		throws KeeperException, InterruptedException {
		// TODO - to be improved to support retry getting

		return zk.getData(path, watcher, stat);
	}

	public List<String> getChildren(String path) throws KeeperException, InterruptedException {
		return this.zk.getChildren(path, true);
	}

	public ZooKeeper getClient() {
		return this.zk;
	}

	public void close() {
		try {
			zk.close();
		} catch (InterruptedException e) {
			LOG.error(e);
		}
	}
}
