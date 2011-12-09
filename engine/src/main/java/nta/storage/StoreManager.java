/**
 * 
 */
package nta.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.MemStore;

import nta.catalog.TableInfo;
import nta.catalog.proto.TableProtos.TableProto;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.util.FileUtils;

/**
 * TRID에 해당하는 파일의 Scanner를 생성해 준다.
 * 
 * @author Hyunsik Choi
 *
 */
public class StoreManager {
	private final Log LOG = LogFactory.getLog(StoreManager.class);

	private final NtaConf conf;
	private final FileSystem defaultFS;
	private final Path dataRootPath;
	
	private final MemStores memStores;

	public StoreManager(NtaConf conf, FileSystem defaultFS) {
		this.conf = conf;
		this.defaultFS = defaultFS;
		this.dataRootPath = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
		this.memStores = new MemStores(conf);
		LOG.info("Storage Manager initialized");
	}

	/**
	 * data root에 테이블 디렉토리 생성
	 * 
	 * @param name
	 * @param meta
	 * @param conf
	 * @return
	 * @throws IOException 
	 */
	public Store create(TableInfo meta) throws IOException {
		try {
			switch (meta.getStoreType()) {
			case MEM: {
				return createMemTable(new URI("mem://"+meta.getName()), meta); 
			}			  
			case RAW: {
				Path tableUri = null;		
				tableUri = new Path(dataRootPath, meta.getName());
				return create(tableUri.toUri(), meta);
			}
			default : return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public synchronized Store createMemTable(URI tableUri, TableInfo meta) {		
		Store store = new Store(tableUri, meta);
		MemTable memTable = new MemTable(store);
		memStores.addMemStore(store, memTable);
		return store;
	}
	
	public synchronized Store create(URI tableUri, TableInfo meta) throws IOException {
		Path tablePath = new Path(tableUri);

		if(defaultFS.exists(tablePath) == true) {
			throw new StoreAlreadyExistsException(tableUri.toString());
		}

		defaultFS.mkdirs(tablePath);
		Path metaFile = new Path(tablePath, NConstants.ENGINE_TABLEMETA_FILENAME);
		FSDataOutputStream out = defaultFS.create(metaFile);
		FileUtils.writeProto(out, meta.getProto());
		out.flush();
		out.close();
		
		return new Store(tablePath.toUri(), meta);
	}

	public synchronized Store open(URI tableUri) throws IOException {
		TableInfo meta = null;

		Path tablePath = new Path(tableUri);
		FileSystem fs = tablePath.getFileSystem(conf);
		
		Path tableMetaPath = new Path(tablePath, ".meta");
		if(!fs.exists(tablePath)) {
			throw new FileNotFoundException(".meta file not found in "+tableUri.toString());
		}
		FSDataInputStream tableMetaIn = 
			fs.open(tableMetaPath);

		TableProto tableProto = (TableProto) FileUtils.loadProto(tableMetaIn, 
			TableProto.getDefaultInstance());
		meta = new TableInfo(tableProto);

		return new Store(tablePath.toUri(), meta);
	}
	
	public synchronized void delete(Store store) throws IOException {
		switch(store.getStoreType()) {
		case RAW:
		case CSV: {
			Path tablePath = new Path(store.getURI());
			FileSystem fs = tablePath.getFileSystem(conf);
			fs.delete(tablePath, true);
			break;
		}
		case MEM: {
			memStores.dropMemStore(store);
			break;
		}
		}		
	}

	public Scanner getScanner(Store store) throws IOException {
		Scanner scanner = null;
		
		switch(store.getStoreType()) {
		case MEM:
			scanner = memStores.getMemStore(store);
			break;
		case RAW:
			scanner = new RawFile(conf, store);
			break;
		case CSV:
			scanner = new CSVFile(conf, store);				
			break;
		default: return null;
		}
		
		scanner.init();

		return scanner;
	}
	
	public UpdatableScanner getUpdatableScanner(Store store) throws IOException {
		UpdatableScanner scanner = null;
		
		switch(store.getStoreType()) {
		case MEM:
			scanner = memStores.getMemStore(store);
			break;
		case RAW:
			scanner = new RawFile(conf, store);
			break;				
		default: return null;
		}
		
		scanner.init();
		
		return scanner;
	}
}
