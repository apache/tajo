package nta.zookeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.util.FileUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class ZkServer {
	private final static Log LOG = LogFactory.getLog(ZkServer.class);
	
	private ZooKeeperServer zkServer;
	private Factory factory;
	private final String dataDir;
	private final String logDir;	
	private final int tickTime;
	private final int port;
	private final int sessionTimeout;
	
	public ZkServer(Configuration conf) throws IOException {		
		this.port = conf.getInt(NConstants.ZOOKEEPER_PORT, NConstants.DEFAULT_ZOOKEEPER_PORT);
		
		this.dataDir = conf.get(NConstants.ZOOKEEPER_DATA_DIR);
		this.logDir = conf.get(NConstants.ZOOKEEPER_LOG_DIR);
		
		this.tickTime = conf.getInt(NConstants.ZOOKEEPER_TICK_TIME, NConstants.DEFAULT_ZOOKEEPER_TICK_TIME);
		this.sessionTimeout = conf.getInt(NConstants.ZOOKEEPER_SESSION_TIMEOUT, 
				NConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
	}
	
	public void start() throws IOException {
		LOG.info("Starting Local Zookeeper Server (localhost:"+port+")");
		
		startSingleZkServer(				
				FileUtil.getFile(dataDir),
				FileUtil.getFile(logDir),
				tickTime,
				port
				);
	}
	
	private void startSingleZkServer(final File dataDir, final File dataLogDir, 
			final int tickTime, final int port) throws IOException {
        try {
            zkServer = new ZooKeeperServer(dataDir, dataLogDir, tickTime);
            zkServer.setMinSessionTimeout(this.sessionTimeout);
            factory = new NIOServerCnxn.Factory(new InetSocketAddress(port));
            factory.startup(zkServer);
        } catch (Exception e) {
        	throw new IOException(e.getCause());
		}
    }
	
	public void shutdown() {
		zkServer.shutdown();
	}
	
	public static void main(String [] args) throws IOException {
		NtaConf conf = new NtaConf();
		ZkServer server = new ZkServer(conf);
		server.start();
		
		server.shutdown();
	}
}
