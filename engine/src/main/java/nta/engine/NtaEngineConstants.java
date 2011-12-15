package nta.engine;

public interface NtaEngineConstants {
	public static String MASTER_PORT="nta.engine.addr";
	public static int MASTER_PORT_DEFAULT=9001;
	
	public static String LEAFSERVER_PORT="nta.engine.addr";
	public static int LEAFSERVER_PORT_DEFAULT=10001;
	
	public static String CLUSTER_DISTRIBUTED="nta.cluster.distributed";
	public static String CLUSTER_IS_LOCAL="false";
	
	public static String ENGINE_BASE_DIR="nta.engine.basedir";
	public static String ENGINE_BASE_DIR_DEFAULT="/tmp/nta";
	
	public static String ENGINE_CATALOG_DIR="engine.catlog.dir";
	public static String ENGINE_CATALOG_DIR_DEFAULT="catalog";
	public static String ENGINE_CATALOG_WALFILE="catalog.wal";
	
	public static String ENGINE_DATA_DIR="engine.data.dir";
	public static String ENGINE_DATA_DIR_DEFAULT="/nta";
	
	public static String ENGINE_CATALOG_FILENAME = "catalog.tex";
	public static String ENGINE_TABLEMETA_FILENAME = ".meta";
}
