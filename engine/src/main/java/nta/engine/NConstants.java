package nta.engine;

public final class NConstants {
	public static final String MASTER_ADDRESS="engine.master.addr";
	public static final String DEFAULT_MASTER_ADDRESS="localhost:9001";

	public static final int DEFAULT_MASTER_PORT=9001;
	
  public static final String CATALOG_ADDRESS = "catalog.master.addr";
  public static final String DEFAULT_CATALOG_ADDRESS = "0.0.0.0:9002";
	
	public static final String LEAFSERVER_PORT="engine.leafserver.port";
	public static final int DEFAULT_LEAFSERVER_PORT=9003;
	
	public static final String CLIENT_SERVICE_ADDRESS="client.service.addr";
	public static final String DEFAULT_CLIENT_SERVICE_ADDRESS="localhost:9004";
  public static final int DEFAULT_CLIENT_SERVICE_PORT = 9004;
	
	public static final String ZOOKEEPER_ADDRESS="zookeeper.server.addr";
	public static final String DEFAULT_ZOOKEEPER_ADDRESS="localhost:2181";		
	
	public static final String ZOOKEEPER_TICK_TIME="zookeeper.server.ticktime";
	public static final int DEFAULT_ZOOKEEPER_TICK_TIME=5000;
	public static final String ZOOKEEPER_SESSION_TIMEOUT="zookeeper.session.timeout";
	public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT=180*1000;	
	
	// the two constants are only used for local zookeeper
	public static final String ZOOKEEPER_DATA_DIR="zookeeper.server.datadir";
	public static final String ZOOKEEPER_LOG_DIR="zookeeper.server.logdir";

  public static final String ZOOKEEPER_RETRY_COUNT="zookeeper.retry.count";
  public static final int DEFAULT_ZOOKEEPER_RETRY_COUNT=3;

  public static final String ZOOKEEPER_RETRY_INTERVALMILLS="zookeeper.retry.intervalmills";
  public static final int DEFAULT_ZOOKEEPER_RETRY_INTERVALMILLS=1000;
	
	public static final String CLUSTER_DISTRIBUTED="tajo.cluster.distributed";
	public static final String CLUSTER_IS_LOCAL="false";
	
	public static final String ENGINE_BASE_DIR="engine.rootdir";
	
	public static final String ENGINE_DATA_DIR="engine.data.dir";
	
	public static final String WORKER_BASE_DIR="tajo.worker.basedir";
	
	public static final String WORKER_TMP_DIR="tajo.worker.tmpdir";
	
	public static final String ZNODE_BASE="/nta";
	public static final String ZNODE_MASTER="/nta/master";
	public static final String ZNODE_LEAFSERVERS="/nta/leafservers";
	public static final String ZNODE_QUERIES="/nta/queries";
	public static final String ZNODE_CLIENTSERVICE="/nta/clientservice";
	
	public static final String ZNODE_CATALOG="/catalog";

	public static final String RAWFILE_SYNC_INTERVAL = "rawfile.sync.interval";

  public static final String EXTERNAL_SORT_BUFFER = "tajo.extsort.buffer";

	private NConstants() {

	}
}
