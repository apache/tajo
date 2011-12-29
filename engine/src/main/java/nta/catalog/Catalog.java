package nta.catalog;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import nta.catalog.exception.AlreadyExistsFunction;
import nta.catalog.exception.AlreadyExistsTableException;
import nta.catalog.exception.NoSuchFunctionException;
import nta.catalog.exception.NoSuchTableException;
import nta.catalog.proto.TableProtos.StoreType;
import nta.conf.NtaConf;
import nta.engine.EngineService;
import nta.engine.NConstants;
import nta.engine.function.FuncType;
import nta.engine.function.TestFunc;
import nta.engine.function.UnixTimeFunc;
import nta.engine.ipc.protocolrecords.Tablet;
import nta.engine.query.LocalEngine;
import nta.storage.Store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Hyunsik Choi
 */
public class Catalog implements EngineService {
	private static Log LOG = LogFactory.getLog(Catalog.class);
	private NtaConf conf;	
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private Lock rlock = lock.readLock();
	private Lock wlock = lock.writeLock();

	private AtomicInteger newRelId = new AtomicInteger(0);
	private Map<Integer, TableMeta> tables = new HashMap<Integer, TableMeta>();
	private Map<Integer, List<TabletInfo>> tabletServingInfo = new HashMap<Integer, List<TabletInfo>>();
	private Map<String, Integer> tablesByName = new HashMap<String, Integer>();	
	private Map<String, FunctionMeta> functions = new HashMap<String, FunctionMeta>();	

	private SimpleWAL wal;
	private Logger logger;
	private String catalogDirPath;
	private File walFile;

	TimeseriesRelations baseRelations;
	TimeseriesRelations cubeRelations;

	public Catalog(NtaConf conf) {
		this.conf = conf;
		
		FunctionMeta funcMeta = new FunctionMeta("print", TestFunc.class, FuncType.GENERAL);
		registerFunction(funcMeta);
		funcMeta = new FunctionMeta("time", UnixTimeFunc.class, FuncType.GENERAL);
		registerFunction(funcMeta);	
	}

	public void init() throws IOException {		
		catalogDirPath = conf.get(NConstants.ENGINE_CATALOG_DIR);		
		
		File catalogDir = new File(catalogDirPath);
		if(!catalogDir.exists()) {
			catalogDir.mkdirs();
		}
		
		walFile = new File(catalogDir+"/"+NConstants.ENGINE_CATALOG_WALFILE);
		wal = new SimpleWAL(walFile);
		this.logger = new Logger(wal);
	}
	
	private static void writeMetaToFile(Writer writer, TableMeta meta) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("+t\t");
		sb.append(meta.getName()).append("\t");
		sb.append(meta.getStore().getURI().toString());
		sb.append("\n");
		
		writer.append(sb.toString());
	}

	public TableInfo getTableInfo(int relationId) {
		rlock.lock();
		try {
			return this.tables.get(relationId);
		} finally { 
			rlock.unlock(); 
		}		
	}

	public TableInfo getTableInfo(String relationName) throws NoSuchTableException {
		rlock.lock();
		try {
			if (!this.tablesByName.containsKey(relationName)) {
				throw new NoSuchTableException(relationName);
			}
			int rid = this.tablesByName.get(relationName);
			return this.tables.get(rid);
		} finally {
			rlock.unlock();
		}
	}

	public Collection<TableInfo> getTableInfos() {
		wlock.lock();
		try {
			return new ArrayList<TableInfo>(tables.values());
		} finally {
			wlock.unlock();
		}
	}
	
	public void resetHostsByTable() {
		this.tabletServingInfo.clear();
	}
	
	public List<TabletInfo> getHostByTable(String tableName) {
		return tabletServingInfo.get(tablesByName.get(tableName));
	}
	
	public void updateAllTabletServingInfo() throws IOException {
		Collection<TableMeta> tbs = tables.values();
		Iterator<TableMeta> it = tbs.iterator();
		while (it.hasNext()) {
			updateTabletServingInfo(it.next());
		}
	}
	
	public void updateTabletServingInfo(TableInfo info) throws IOException {
		int fileIdx, blockIdx;
		FileSystem fs = FileSystem.get(conf);
		Store store = info.getStore();
		Path path = new Path(store.getURI()+"/data");
		
		FileStatus[] files = fs.listStatus(path);
		BlockLocation[] blocks;
		String[] hosts;
		List<TabletInfo> tabletInfoList;
		int tid = tablesByName.get(info.getName());
		if (tabletServingInfo.containsKey(tid)) {
			tabletInfoList = tabletServingInfo.get(tid);
		} else {
			tabletInfoList = new ArrayList<TabletInfo>();
		}
		
		for (fileIdx = 0; fileIdx < files.length; fileIdx++) {
			blocks = fs.getFileBlockLocations(files[fileIdx], 0, files[fileIdx].getLen());
			for (blockIdx = 0; blockIdx < blocks.length; blockIdx++) {
				hosts = blocks[blockIdx].getHosts();
				if (tabletServingInfo.containsKey(tid)) {
					tabletInfoList = tabletServingInfo.get(tid);
				} else {
					tabletInfoList = new ArrayList<TabletInfo>();
				}
				// TODO: select the proper serving node for block
				tabletInfoList.add(new TabletInfo(hosts[0], new Tablet(files[fileIdx].getPath(), 
						blocks[blockIdx].getOffset(), blocks[blockIdx].getLength())));
			}
			tabletServingInfo.put(tid, tabletInfoList);
		}
	}

	public void addTable(TableMeta meta) throws AlreadyExistsTableException {
		wlock.lock();

		try {
			if (tablesByName.containsKey(meta.getName())) {
				throw new AlreadyExistsTableException(meta.getName());
			}

			int newTableId = newRelId.getAndIncrement();
			this.tables.put(newTableId, meta);
			this.tablesByName.put(meta.getName(), newTableId);

			if(this.logger != null && (meta.getStoreType() != StoreType.MEM))
				this.logger.appendAddTable(meta);
			
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		} finally {
			wlock.unlock();
		}
	}

	public TableInfo deleteTable(String name) throws NoSuchTableException {
		wlock.lock();
		try {
			if (!tablesByName.containsKey(name)) {
				throw new NoSuchTableException(name);
			}

			if(this.logger != null)
				this.logger.appendDelTable(name);

			int id = tablesByName.remove(name);
			return tables.remove(id);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return null;
		}
		finally {
			wlock.unlock();
		}
	}

	public boolean existsTable(String name) {
		rlock.lock();
		try {
			return tablesByName.containsKey(name);
		} finally {
			rlock.unlock();
		}
	}

	public synchronized int getTableId(String relName) throws NoSuchTableException {
		rlock.lock();
		try {
			if (!tablesByName.containsKey(relName))
				throw new NoSuchTableException(relName);

			return tablesByName.get(relName);
		} finally {
			rlock.unlock();
		}
	}

	public void registerFunction(FunctionMeta funcMeta) {
		if (functions.containsKey(funcMeta.name)) {
			throw new AlreadyExistsFunction(funcMeta.name);
		}

		functions.put(funcMeta.name, funcMeta);
		if(this.logger != null) {
			try {
				this.logger.appendAddFunction(funcMeta);
			} catch (IOException e) {
				LOG.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public void unregisterFunction(String funcName) {
		if (!functions.containsKey(funcName)) {
			throw new NoSuchFunctionException(funcName);
		}

		if(logger != null) {
			try {
				this.logger.appendDelFunction(funcName);
			} catch (IOException e) {
				LOG.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		functions.remove(funcName);
	}

	public FunctionMeta getFunctionMeta(String funcName) {
		return this.functions.get(funcName);
	}

	public boolean containFunction(String funcName) {
		return this.functions.containsKey(funcName);
	}

	public Collection<FunctionMeta> getFunctions() {
		return this.functions.values();
	}

	private static class Logger implements CatalogWALService {
		SimpleWAL wal;
		public Logger(SimpleWAL wal) {
			this.wal = wal;
		}
		public void appendAddTable(TableMeta meta) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append("+t\t");
			sb.append(meta.getName()).append("\t");
			sb.append(meta.getStore().getURI().toString());

			wal.append(sb.toString());
		}

		public void appendDelTable(String name) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append("-t\t");
			sb.append(name);
			wal.append(sb.toString());
		}

		public void appendAddFunction(FunctionMeta meta) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append("+f\t");
			sb.append(meta.getName()).append("\t");
			sb.append(meta.getType()).append("\t");
			sb.append(meta.getFunctionClass().getCanonicalName()).append("\t");

			wal.append(sb.toString());
		}

		public void appendDelFunction(String name) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append("-f\t");
			sb.append(name);
			
			wal.append(sb.toString());
		}
	}

	@Override
	public void shutdown() throws IOException {
		LOG.info(LocalEngine.class.getName()+" is being stopped");
		
		File catalogFile = new File(catalogDirPath+"/"+NConstants.ENGINE_CATALOG_FILENAME);
		if(catalogFile.exists()) {
			catalogFile.delete();
		}
		OutputStream os = new FileOutputStream(catalogFile);
		OutputStreamWriter osw = new OutputStreamWriter(os);
		BufferedWriter writer = new BufferedWriter(osw);
		
		for(TableMeta meta : tables.values()) {
			writeMetaToFile(writer, meta);
		}
		
		writer.flush();
		osw.flush();
		os.flush();
		os.close();		
		wal.close();

		if(walFile.exists()) {
			walFile.delete();
		}
	} 
}
