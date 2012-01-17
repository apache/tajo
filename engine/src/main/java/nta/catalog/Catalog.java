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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import nta.catalog.exception.AlreadyExistsFunction;
import nta.catalog.exception.AlreadyExistsTableException;
import nta.catalog.exception.NoSuchFunctionException;
import nta.catalog.exception.NoSuchTableException;
import nta.catalog.proto.TableProtos.StoreType;
import nta.engine.EngineService;
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Tablet;
import nta.engine.query.LocalEngine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

/**
 * @author Hyunsik Choi
 */
public class Catalog implements CatalogService, EngineService {
	private static Log LOG = LogFactory.getLog(Catalog.class);
	private Configuration conf;	
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private Lock rlock = lock.readLock();
	private Lock wlock = lock.writeLock();

	private AtomicInteger newRelId = new AtomicInteger(0);
	private Map<Integer, TableDesc> tables = new HashMap<Integer, TableDesc>();
	private Map<Integer, List<TabletServInfo>> tabletServingInfo = new HashMap<Integer, List<TabletServInfo>>();
	private Map<String, Integer> tablesByName = new HashMap<String, Integer>();	
	private Map<String, FunctionDesc> functions = new HashMap<String, FunctionDesc>();	

	private SimpleWAL wal;
	private Logger logger;
	private String catalogDirPath;
	private File walFile;

	public Catalog(Configuration conf) {
		this.conf = conf;
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
	
	private static void writeMetaToFile(Writer writer, TableDesc meta) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("+t\t");
		sb.append(meta.getName()).append("\t");
		sb.append(meta.getURI().toString());
		sb.append("\n");
		
		writer.append(sb.toString());
	}

	public TableDesc getTableDesc(int relationId) {
		rlock.lock();
		try {
			return this.tables.get(relationId);
		} finally { 
			rlock.unlock(); 
		}		
	}

	public TableDesc getTableDesc(String relationName) throws NoSuchTableException {
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

	public Collection<TableDesc> getAllTableDescs() {
		wlock.lock();
		try {
			return new ArrayList<TableDesc>(tables.values());
		} finally {
			wlock.unlock();
		}
	}
	
	public void resetHostsByTable() {
		this.tabletServingInfo.clear();
	}
	
	public List<TabletServInfo> getHostByTable(String tableName) {
		return tabletServingInfo.get(tablesByName.get(tableName));
	}
	
	public void updateAllTabletServingInfo(List<String> onlineServers) throws IOException {
		tabletServingInfo.clear();
		Collection<TableDesc> tbs = tables.values();
		Iterator<TableDesc> it = tbs.iterator();
		List<TabletServInfo> locInfos;
		List<TabletServInfo> servInfos;
		int index = 0;
		StringTokenizer tokenizer;
		while (it.hasNext()) {
			TableDesc td = it.next();
			locInfos = getTabletLocInfo(td);
			servInfos = new ArrayList<TabletServInfo>();
			// TODO: select the proper online server
			for (TabletServInfo servInfo : locInfos) {
				// round robin
				if (index == onlineServers.size()) {
					index = 0;
				}
				tokenizer = new StringTokenizer(onlineServers.get(index++), ":");
				servInfo.setHost(tokenizer.nextToken(), Integer.valueOf(tokenizer.nextToken()));
				servInfos.add(servInfo);
			}
			tabletServingInfo.put(td.getId(), servInfos);
		}
	}
	
	private List<TabletServInfo> getTabletLocInfo(TableDesc desc) throws IOException {
		int fileIdx, blockIdx;
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(desc.getURI());
		
		FileStatus[] files = fs.listStatus(new Path(path+"/data"));
		BlockLocation[] blocks;
		String[] hosts;
		List<TabletServInfo> tabletInfoList = new ArrayList<TabletServInfo>();
		int tid = tablesByName.get(desc.getName());
//		if (tabletServingInfo.containsKey(tid)) {
//			tabletInfoList = tabletServingInfo.get(tid);
//		} else {
//			tabletInfoList = new ArrayList<TabletServInfo>();
//		}
		
		int i=0;
		for (fileIdx = 0; fileIdx < files.length; fileIdx++) {
			blocks = fs.getFileBlockLocations(files[fileIdx], 0, files[fileIdx].getLen());
			for (blockIdx = 0; blockIdx < blocks.length; blockIdx++) {
				hosts = blocks[blockIdx].getHosts();
//				if (tabletServingInfo.containsKey(tid)) {
//					tabletInfoList = tabletServingInfo.get(tid);
//				} else {
//					tabletInfoList = new ArrayList<TabletServInfo>();
//					tabletServingInfo.put(tid, tabletInfoList);
//				}
				// TODO: select the proper serving node for block
				tabletInfoList.add(new TabletServInfo(hosts[0], -1, new Tablet(desc.getName()+"_"+i, 
            files[fileIdx].getPath(), desc.getMeta(), 
						blocks[blockIdx].getOffset(), blocks[blockIdx].getLength())));
				i++;
			}
		}
		return tabletInfoList;
	}
	
	public void addTable(String name, TableMeta info) throws AlreadyExistsTableException {	  
	  addTable(new TableDescImpl(name, info));
	}

	public void addTable(TableDesc desc) throws AlreadyExistsTableException {
		Preconditions.checkNotNull(desc.getURI(), "Must be set to the table URI");
		Preconditions.checkNotNull(desc.getName(), "Must be set to the table name");
	  wlock.lock();
		
		try {
			if (tablesByName.containsKey(desc.getName())) {
				throw new AlreadyExistsTableException(desc.getName());
			}

			// get new relation id
      int newTableId = newRelId.getAndIncrement();      
      
      // set the relation id to TableDesc and its columns
      desc.setId(newTableId);
      
			this.tables.put(newTableId, desc);
			this.tablesByName.put(desc.getName(), newTableId);

			if(this.logger != null && (desc.getMeta().getStoreType() != StoreType.MEM))
				this.logger.appendAddTable(desc);
			
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		} finally {
			wlock.unlock();
		}
	}

	public void deleteTable(String name) throws NoSuchTableException {
		wlock.lock();
		try {
			if (!tablesByName.containsKey(name)) {
				throw new NoSuchTableException(name);
			}

			if(this.logger != null)
				this.logger.appendDelTable(name);

			tablesByName.remove(name);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
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

	public void registerFunction(FunctionDesc funcMeta) {
		if (functions.containsKey(funcMeta.getName())) {
			throw new AlreadyExistsFunction(funcMeta.getName());
		}

		functions.put(funcMeta.getName(), funcMeta);
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

	public FunctionDesc getFunctionMeta(String funcName) {
		return this.functions.get(funcName);
	}

	public boolean containFunction(String funcName) {
		return this.functions.containsKey(funcName);
	}

	public Collection<FunctionDesc> getFunctions() {
		return this.functions.values();
	}

	private static class Logger implements CatalogWALService {
		SimpleWAL wal;
		public Logger(SimpleWAL wal) {
			this.wal = wal;
		}
		public void appendAddTable(TableDesc meta) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append("+t\t");
			sb.append(meta.getName()).append("\t");
			sb.append(meta.getURI().toString());

			wal.append(sb.toString());
		}

		public void appendDelTable(String name) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append("-t\t");
			sb.append(name);
			wal.append(sb.toString());
		}

		public void appendAddFunction(FunctionDesc meta) throws IOException {
			StringBuilder sb = new StringBuilder();
			sb.append("+f\t");
			sb.append(meta.getName()).append("\t");
			sb.append(meta.getType()).append("\t");
			sb.append(meta.getFuncClass().getCanonicalName()).append("\t");

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
		
		for(TableDesc meta : tables.values()) {
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
