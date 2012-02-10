package nta;

import java.io.IOException;
import java.util.TimerTask;

import nta.conf.NtaConf;
import nta.engine.ipc.QueryEngineInterface;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;



public class TableUpdator extends TimerTask {
	private static final Log LOG = LogFactory.getLog(TableUpdator.class);

	private QueryEngineInterface engine;
	private String[] commands;

	public static final String BASETABLE_DIR = "basetable.dir";
	public static final String BASETABLE_PREFIX = "basetable.prefix";
	public static final String HDFS_NAME = "fs.default.name";
	public static final String SERVER_ADDR = "server.addr";
	public static final String SERVER_PORT = "server.port";
	public static final String LOG_RECEIVER_PATH = "logreceiver.path";
	public static final String TABLE_BUILDER_PATH = "tablebuilder.path";
	public static final String STORE_TYPE = "store.type";
	
	public static final String DEFAULT_BASETABLE_DIR = "/basetable";
	public static final String DEFAULT_BASETABLE_PREFIX = "b_";
	public static final String DEFAULT_HDFS_NAME = "localhost:8020";
	public static final String DEFAULT_SERVER_ADDR = "localhost";
	public static final int DEFAULT_SERVER_PORT = 22222;
	public static final String DEFAULT_LOG_RECEIVER_PATH = "src/main/resources/logreceiver.jar";
	public static final String DEFAULT_TABLE_BUILDER_PATH = "src/main/resources/tablebuilder.jar";
	public static final String DEFAULT_STORE_TYPE = "raw";
	
	final String serverAddr;
	final int serverPort;
	final String hdfsAddr;
	final String basetableDir;
	final String basetablePrefix;
	final String logreceiverJar;
	final String tablebuilderJar;
	final String storeType;
	
	public TableUpdator(NtaConf conf, QueryEngineInterface engine) 
			throws IOException {
		this.hdfsAddr = conf.get(TableUpdator.HDFS_NAME, TableUpdator.DEFAULT_HDFS_NAME);
		this.basetableDir = conf.get(TableUpdator.BASETABLE_DIR, 
				TableUpdator.DEFAULT_BASETABLE_DIR);
		this.basetablePrefix = conf.get(TableUpdator.BASETABLE_PREFIX, 
				TableUpdator.DEFAULT_BASETABLE_PREFIX);
		this.serverAddr = conf.get(TableUpdator.SERVER_ADDR, TableUpdator.DEFAULT_SERVER_ADDR);
		this.serverPort = conf.getInt(TableUpdator.SERVER_PORT, TableUpdator.DEFAULT_SERVER_PORT);
		this.logreceiverJar = conf.get(TableUpdator.LOG_RECEIVER_PATH, 
				TableUpdator.DEFAULT_LOG_RECEIVER_PATH);
		this.tablebuilderJar = conf.get(TableUpdator.TABLE_BUILDER_PATH, 
				TableUpdator.DEFAULT_TABLE_BUILDER_PATH);
		this.storeType = conf.get(TableUpdator.STORE_TYPE, TableUpdator.DEFAULT_STORE_TYPE);

		this.engine = engine;
		this.commands = new String[3];
		this.commands[0] = "/bin/sh";
		this.commands[1] = "-c";
	}

	@Override
	public void run() {
		try {
			String tbPath = this.basetableDir + "/" + this.basetablePrefix + System.currentTimeMillis();
			
			this.commands[2] = "java -jar " + this.logreceiverJar + " " + this.serverAddr + " " + this.serverPort + 
					" | flow-export -f4 | " +
					"java -jar " + this.tablebuilderJar + " " + this.hdfsAddr + " " + tbPath + " " + this.storeType;
			
			LOG.info(this.commands[2]);
			
			Process p = Runtime.getRuntime().exec(this.commands);
			p.waitFor();
			
			// register the basetable
			registerTable(new Path(tbPath));
		} catch (Exception e) {
			LOG.error("Exception occured for receiving logs: ", e);
		}
	}

	private void registerTable(Path tbPath) throws Exception {
		this.engine.attachTable(tbPath.getName(), tbPath.toString());
	}
}
