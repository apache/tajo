package nta.zookeeper;

import java.io.File;
import java.io.IOException;

import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.util.FileUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
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
	private final String serverAddr;
	private final int sessionTimeout;
	
	public ZkServer(Configuration conf) throws IOException {	  
		this.serverAddr = 
		    conf.get(NConstants.ZOOKEEPER_ADDRESS, 
		        NConstants.DEFAULT_ZOOKEEPER_ADDRESS);
		
		this.dataDir = conf.get(NConstants.ZOOKEEPER_DATA_DIR);
		LOG.info("Zookeeper data dir is set (" + this.dataDir + ")");
		this.logDir = conf.get(NConstants.ZOOKEEPER_LOG_DIR);
		LOG.info("Zookeeper log dir is set (" + this.logDir + ")");
		
		this.tickTime = conf.getInt(NConstants.ZOOKEEPER_TICK_TIME, 
		    NConstants.DEFAULT_ZOOKEEPER_TICK_TIME);
		this.sessionTimeout = conf.getInt(NConstants.ZOOKEEPER_SESSION_TIMEOUT, 
				NConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
	}
	
	public void start() throws IOException {
		LOG.info("Starting Local Zookeeper Server ("+serverAddr+")");
		
		startSingleZkServer(
				FileUtil.getFile(dataDir),
				FileUtil.getFile(logDir),
				tickTime,
				serverAddr
				);
	}
	
	private void startSingleZkServer(final File dataDir, final File dataLogDir, 
			final int tickTime, final String serverAddr) throws IOException {
        try {
            zkServer = new ZooKeeperServer(dataDir, dataLogDir, tickTime);
            zkServer.setMinSessionTimeout(this.sessionTimeout);
            factory = new NIOServerCnxn.Factory(NetUtils.createSocketAddr(serverAddr));
            factory.startup(zkServer);
        } catch (Exception e) {
        	throw new IOException(e.getCause());
		}
    }
	
	public void shutdown() {
		zkServer.shutdown();
    while (zkServer.isRunning());
	}
	
	public static void main(String [] args) throws IOException {
		NtaConf conf = NtaConf.create();
		ZkServer server = new ZkServer(conf);
		server.start();
	}
}
