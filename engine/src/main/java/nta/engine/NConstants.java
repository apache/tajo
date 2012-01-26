package nta.engine;

public final class NConstants {
	public static String MASTER_HOST="engine.master.addr";
	public static String DEFAULT_MASTER_HOST="local";
	
	public static String MASTER_PORT="engine.master.port";
	public static int DEFAULT_MASTER_PORT=9001;
	
	public static String LEAFSERVER_HOST="engine.leafserver.addr";
	public static String DEFAULT_LEAFSERVER_HOST="0.0.0.0";
	
  public static String CATALOG_MASTER_HOST = "catalog.master.addr";
  public static String DEFAULT_CATALOG_MASTER_HOST = "0.0.0.0";

  public static String CATALOG_MASTER_PORT = "catalog.master.port";
  public static int DEFAULT_CATALOG_MASTER_PORT = 9002;
	
	public static String LEAFSERVER_PORT="engine.leafserver.port";
	public static int DEFAULT_LEAFSERVER_PORT=9010;
	
	public static String ZOOKEEPER_HOST="zookeeper.server.addr";
	public static String DEFAULT_ZOOKEEPER_HOST="localhost";	
	public static String ZOOKEEPER_PORT="zookeeper.server.port";
	public static int DEFAULT_ZOOKEEPER_PORT=2181;	
	
	public static String ZOOKEEPER_TICK_TIME="zookeeper.server.ticktime";
	public static int DEFAULT_ZOOKEEPER_TICK_TIME=5000;
	public static String ZOOKEEPER_SESSION_TIMEOUT="zookeeper.session.timeout";
	public static int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT=180*1000;	
	// the two constants are only used for local zookeeper
	public static String ZOOKEEPER_DATA_DIR="zookeeper.server.datadir";
	public static String ZOOKEEPER_LOG_DIR="zookeeper.server.logdir";
	
	public static String CLUSTER_DISTRIBUTED="nta.cluster.distributed";
	public static String CLUSTER_IS_LOCAL="false";
	
	public static String ENGINE_BASE_DIR="engine.rootdir";
	
	public static String ENGINE_CATALOG_DIR="engine.catalog.dir";	
	public static String ENGINE_CATALOG_WALFILE="catalog.wal";
	
	public static String ENGINE_DATA_DIR="engine.data.dir";
	
	public static String ENGINE_CATALOG_FILENAME = "catalog.tex";
	public static String ENGINE_TABLEMETA_FILENAME = ".meta";
	
	public static String ZNODE_BASE="/nta";
	public static String ZNODE_MASTER="/nta/master";
	public static String ZNODE_LEAFSERVERS="/nta/leafservers";
	public static String ZNODE_QUERIES="/nta/queries";

	public static String RAWFILE_SYNC_INTERVAL = "rawfile.sync.interval";	

	private NConstants() {		
	}
}
