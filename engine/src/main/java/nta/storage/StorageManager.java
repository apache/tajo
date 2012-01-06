/**
 * 
 */
package nta.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nta.catalog.TableDesc;
import nta.catalog.TableMeta;
import nta.catalog.TableUtil;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Tablet;
import nta.util.FileUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * TRID에 해당하는 파일의 Scanner를 생성해 준다.
 * 
 * @author Hyunsik Choi
 *
 */
public class StorageManager {
	private final Log LOG = LogFactory.getLog(StorageManager.class);

	private final NtaConf conf;
	private final FileSystem defaultFS;
	private final Path dataRootPath;
	
	private final MemStores memStores;

	public StorageManager(NtaConf conf, FileSystem defaultFS) {
		this.conf = conf;
		this.defaultFS = defaultFS;
		this.dataRootPath = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
		this.memStores = new MemStores(conf);
		LOG.info("Storage Manager initialized");
	}
	
//	@Deprecated
//	public synchronized Store create(URI tableUri, TableMetaImpl meta) throws IOException {
//		Path tablePath = new Path(tableUri);
//
//		if(defaultFS.exists(tablePath) == true) {
//			throw new StoreAlreadyExistsException(tableUri.toString());
//		}
//
//		defaultFS.mkdirs(tablePath);
//		Path metaFile = new Path(tablePath, NConstants.ENGINE_TABLEMETA_FILENAME);
//		FSDataOutputStream out = defaultFS.create(metaFile);
//		FileUtil.writeProto(out, meta.getProto());
//		out.flush();
//		out.close();
//		
//		return new Store(tablePath.toUri(), meta);
//	}
//
//	@Deprecated
//	public synchronized Store open(URI tableUri) throws IOException {
//		TableMetaImpl meta = null;
//
//		Path tablePath = new Path(tableUri);
//		FileSystem fs = tablePath.getFileSystem(conf);
//		
//		Path tableMetaPath = new Path(tablePath, ".meta");
//		if(!fs.exists(tableMetaPath)) {
//			throw new FileNotFoundException(".meta file not found in "+tableUri.toString());
//		}
//		FSDataInputStream tableMetaIn = 
//			fs.open(tableMetaPath);
//
//		TableProto tableProto = (TableProto) FileUtil.loadProto(tableMetaIn, 
//			TableProto.getDefaultInstance());
//		meta = new TableMetaImpl(tableProto);
//
//		return new Store(tablePath.toUri(), meta);
//	}
	
	@Deprecated
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
	
	public FileScanner getScanner(Path tablePath) throws IOException {
    TableMeta info = TableUtil.getTableMeta(conf, tablePath);
    Tablet [] tablets = getTablets(tablePath);
    return getScanner(info, tablets);
  }
	
	public FileScanner getScanner(TableDesc meta) throws IOException {
	  Tablet [] tablets = getTablets(new Path(meta.getURI()));
	  return getScanner(meta.getInfo(), tablets);
	}
	
	public FileScanner getScanner(TableMeta info, Tablet [] tablets) throws IOException {
    FileScanner scanner = null;
    
    switch(info.getStoreType()) {
    case RAW: {
      scanner = new RawFile2.RawFileScanner(conf, info.getSchema(), tablets);     
      break;
    }
    case CSV: {
      scanner = new CSVFile2.CSVScanner(conf, info.getSchema(), tablets);
      break;
    }
    }
    
    return scanner;
  }
	
	public Tablet [] getTablets(Path tablePath) throws IOException {
	  
	  Path dataPath = new Path(tablePath,"data");    
    FileStatus[] files = defaultFS.listStatus(dataPath);
    List<Tablet> tablets = new ArrayList<Tablet>();    
    
    long blockSize = defaultFS.getDefaultBlockSize();
    Tablet tablet = null;
    for(FileStatus file : files) {
      // split a file to multiple tablets
      if(file.getLen() > defaultFS.getDefaultBlockSize()) {
        long offset = 0;
        long remain = file.getLen();
        
        while(remain > 0) {
          tablet = new Tablet();
          if(remain > defaultFS.getDefaultBlockSize()) {
            tablet = new Tablet(tablePath, file.getPath().getName(), offset, blockSize);
            remain -= blockSize;
          } else {
            tablet = new Tablet(tablePath, file.getPath().getName(), offset, remain);
            remain = 0;
          }
          tablets.add(tablet);
        }
      } else { // a file is just a tablet
        tablet = new Tablet(tablePath, file.getPath().getName(), 0l, file.getLen());
        tablets.add(tablet);
      }
    }
	  
	  return tablets.toArray(new Tablet[tablets.size()]);
	}
	
	
	
	public Appender getAppender(TableDesc meta) throws IOException {	  
	  Path path = new Path(dataRootPath, meta.getName());    
    return getAppender(meta.getInfo(), path);
	}
	
	/**
	 * 파일을 outputPath에 출력한다. writeMeta가 true라면 TableMeta를 테이블 디렉토리에 저장한다.
	 * 
	 * @param meta 테이블 정보
	 * @param outputPath 테이블을 출력할 디렉토리
	 * @param writeMeta 
	 * @return
	 * @throws IOException
	 */
	public Appender getAppender(TableMeta meta, Path outputPath, boolean writeMeta) throws IOException {	  

    if(defaultFS.exists(outputPath) == true) {
      throw new StoreAlreadyExistsException(outputPath.toString());
    }

    defaultFS.mkdirs(outputPath);
    Path metaFile = new Path(outputPath, NConstants.ENGINE_TABLEMETA_FILENAME);
    FSDataOutputStream out = defaultFS.create(metaFile);
    FileUtil.writeProto(out, meta.getProto());
    out.flush();
    out.close();
    
    return getAppender(meta, outputPath);
	}
	
	public Appender getAppender(TableMeta meta, Path outputPath) throws 
	  IOException {
	 
	  Appender appender = null;
	  
	  switch(meta.getStoreType()) {
	  case RAW: {
	    appender = new RawFile2.RawFileAppender(conf, outputPath, 
	        meta.getSchema());
	    break;
	  }
	  case CSV: {
	    appender = new CSVFile2.CSVAppender(conf, outputPath, meta.getSchema());
	    break;
	  }
	  }
	  
	  FSDataOutputStream out = defaultFS.create(new Path(outputPath,".meta"));
	  FileUtil.writeProto(out, meta.getProto());
	  out.flush();
	  out.close();
	  
	  return appender;
	}
	
	@Deprecated
	public UpdatableScanner getUpdatableScanner(Store store) throws IOException {
		UpdatableScanner scanner = null;
		
		switch(store.getStoreType()) {
/*		case MEM:
			scanner = memStores.getMemStore(store);
			break;*/
		case RAW:
			scanner = new RawFile(conf, store);
			break;				
		default: return null;
		}
		
		scanner.init();
		
		return scanner;
	}
}
