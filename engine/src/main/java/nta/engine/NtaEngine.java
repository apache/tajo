/**
 * 
 */
package nta.engine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.DNS;

import nta.catalog.Catalog;
import nta.catalog.TableInfo;
import nta.catalog.TableMeta;
import nta.catalog.exception.AlreadyExistsTableException;
import nta.catalog.exception.NoSuchTableException;
import nta.conf.NtaConf;
import nta.engine.exception.NTAQueryException;
import nta.engine.query.LocalEngine;
import nta.storage.Store;
import nta.storage.StorageManager;

/**
 * @author hyunsik
 *
 */
public class NtaEngine implements NtaEngineInterface, Runnable {
	private final static Log LOG = LogFactory.getLog(NtaEngine.class); 
	
	private NtaConf conf;
	private FileSystem defaultFS;
	
	private Catalog catalog;
	private StorageManager storeManager;
	private LocalEngine queryEngine;
	
	private final Path basePath;
	private final Path catalogPath;
	private final Path dataPath;
	//private final Path jarsPath;
	
	/**
	 * This servers address.
	 */
	private final InetSocketAddress bindAddr;
	private RPC.Server server;
	
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	PrintStream ps = new PrintStream(baos);
	
	private List<EngineService> services = new ArrayList<EngineService>();
	
	public NtaEngine(NtaConf conf) throws IOException {
		this.conf = conf;	
		
		String master = conf.get(NConstants.MASTER_HOST,"local");
		if("local".equals(master)) {
			// local mode
			this.defaultFS = LocalFileSystem.get(conf);
			LOG.info("LocalFileSystem is initialized.");
		} else {
			// remote mode
			this.defaultFS = FileSystem.get(conf);	
			LOG.info("FileSystem is initialized.");
		}	
		
		this.basePath = new Path(conf.get(NConstants.ENGINE_BASE_DIR));
		LOG.info("Base dir is set " + conf.get(NConstants.ENGINE_BASE_DIR));
		File baseDir = new File(this.basePath.toString());
		if(baseDir.exists() == false) {
			baseDir.mkdir();
			LOG.info("Base dir ("+baseDir.getAbsolutePath()+") is created.");
		}
		
		this.dataPath = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
		LOG.info("Data dir is set " + dataPath);		
		if(!defaultFS.exists(dataPath)) {
			defaultFS.mkdirs(dataPath);
			LOG.info("Data dir ("+dataPath+") is created");
		}		
		this.storeManager = new StorageManager(conf, defaultFS);
		
		this.catalogPath = new Path(conf.get(NConstants.ENGINE_CATALOG_DIR));
		LOG.info("Catalog dir is set to " + this.catalogPath);
		File catalogDir = new File(this.catalogPath.toString());	
		if(catalogDir.exists() == false) {
			catalogDir.mkdir();
			LOG.info("Catalog dir ("+catalogDir.getAbsolutePath()+") is created.");
		}
		this.catalog = new Catalog(conf);
		this.catalog.init();
		File catalogFile = new File(catalogPath+"/"+NConstants.ENGINE_CATALOG_FILENAME);
		if(catalogFile.exists())		
			loadCatalog(catalogFile);
		services.add(catalog);
		
		this.queryEngine = new LocalEngine(conf, catalog, storeManager, null);
		this.queryEngine.init();
		services.add(queryEngine);
		
		String hostname = DNS.getDefaultHost(
		      conf.get("engine.master.dns.interface", "default"),
		      conf.get("engine.master.dns.nameserver", "default"));
		int port = conf.getInt(NConstants.MASTER_PORT, NConstants.DEFAULT_MASTER_PORT);
		// Creation of a HSA will force a resolve.
	    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
	    if (initialIsa.getAddress() == null) {
	      throw new IllegalArgumentException("Failed resolve of " + this.bindAddr);
	    }
	    
	    this.server = RPC.getServer(this, initialIsa.getHostName(), initialIsa.getPort(), conf);
	    this.bindAddr = this.server.getListenerAddress();	    
		LOG.info(NtaEngine.class.getSimpleName()+" is bind to "+bindAddr.getAddress()+":"+bindAddr.getPort());
		
		Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
	}
	
	public void init() throws IOException {
		
			
	}
	
	public void start() {
		this.server.start();
		LOG.info("NtaEngineCluster startup.");
	}
	
	public void shutdown() {
		LOG.info("NtaEngineCluster is being stopping.");
		
		for(EngineService service : services) {
			try {
				service.shutdown();
			} catch (Exception e) {
				LOG.error(e);
			}
		}
	}
	
	private void loadCatalog(File catalogFile) throws IOException {
		FileInputStream fis = new FileInputStream(catalogFile);	
		LineNumberReader reader = new LineNumberReader(new InputStreamReader(fis));
		String line = null;
		
		while((line = reader.readLine()) != null) {
			String [] cols = line.split("\t");
			attachTable(cols[1], new Path(cols[2]));
		}
	}
	
	@Override
	public void run() {
		while(true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private class ShutdownHook implements Runnable {
		@Override
		public void run() {
			shutdown();
		}
	}
	
	///////////////////////////////////////////////////////////////////////////
	// NtaEngineProtocol
	///////////////////////////////////////////////////////////////////////////
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {	
		return 0;
	}
	
	public ResultSetOld executeQuery(String query) throws NTAQueryException {
		return queryEngine.executeQuery(query);
	}

	@Override
	public String executeQueryC(String query) throws NTAQueryException {
		try {
		ResultSetOld res = queryEngine.executeQuery(query);
		if(res == null)
			return "";
		else
			return res.toString();
		} catch (NTAQueryException nqe) {
			return nqe.getMessage();
		}
	}
	
	@Override
	public void updateQuery(String query) throws NTAQueryException {
		queryEngine.updateQuery(query);		
	}

	@Override
	public void createTable(TableMeta meta) throws IOException {
		if(catalog.existsTable(meta.getName()))
			throw new AlreadyExistsTableException(meta.getName());
		
		Store store = storeManager.create(meta);
		meta.setStore(store);
		catalog.addTable(meta);
		LOG.info("Table "+meta.getName()+" ("+meta.getStoreType()+") is created.");
	}

	@Override
	public void dropTable(String name) throws IOException {
		TableInfo info = catalog.deleteTable(name);
		storeManager.delete(info.getStore());
		
		LOG.info("Table "+name+" is dropped.");
	}

	@Override
	public void attachTable(String name, Path path) throws IOException {
		if(catalog.existsTable(name))
			throw new AlreadyExistsTableException(name);
		
		LOG.info(path.toUri());
		Store store = storeManager.open(path.toUri());
		TableMeta meta = new TableMeta(name, store);
		
		catalog.addTable(meta);
		LOG.info("Table "+meta.getName()+" is attached.");
	}

	@Override
	public void detachTable(String name) throws NoSuchTableException {
		if(!catalog.existsTable(name)) {
			throw new NoSuchTableException(name);
		}

		catalog.deleteTable(name);
		LOG.info("Table "+name+" is detached.");
	}
	
	public boolean existsTable(String name) {
		return catalog.existsTable(name);
	}
	
	public TableInfo getTableInfo(String name) throws NoSuchTableException {
		if(!catalog.existsTable(name)) {
			throw new NoSuchTableException(name);
		}
		
		return catalog.getTableInfo(name);
	}
	
	@Override
	public TableInfo[] getTablesByOrder() throws Exception {
		List<TableInfo> tables = new ArrayList<TableInfo>(catalog.getTableInfos());
		Collections.sort(tables, new TableInfo.TableComparator());
		return tables.toArray(new TableInfo[tables.size()]);
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		NtaConf conf = new NtaConf();
		NtaEngine engine = new NtaEngine(conf);
		engine.init();
		engine.start();
	}
}