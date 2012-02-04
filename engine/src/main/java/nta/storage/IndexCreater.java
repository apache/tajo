package nta.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;

import nta.datum.Datum;
import nta.engine.ipc.protocolrecords.Fragment;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * make index for tablet
 * @author Ryu Hyo Seok
 *     
 */
public class IndexCreater implements Closeable{
	public static final int ONE_LEVEL_INDEX = 1;
	public static final int TWO_LEVEL_INDEX = 2;
	
	private FileScanner scanner;
	private NtaConf conf;
	private FSDataOutputStream out;
	private FileSystem fs;
	private Path tablePath;
	private Path parentPath;
	private int storeType;
	private Schema schema;
	private int level;
	private int loadNum = 4096;
	private String fileName;

	/**
	 *  constructor
	 * @param conf
	 * @param tablePath
	 * @param level : : IndexCreater.ONE_LEVEL_INDEX or IndexCreater.TWO_LEVEL_INDEX
	 * @throws IOException
	 */
	public IndexCreater (NtaConf conf, int level) throws IOException  {

		scanner = null;
		this.conf = conf;
		this.level = level;
		
	}
	/**
	 *
	 * @param loadNum
	 */

	public void setLoadNum(int loadNum){
		this.loadNum = loadNum;
	}
	
	/**
	 * 
	 * @param conf
	 * @param tablePath
	 * @param tablets
	 * @return
	 * @throws IOException
	 */
	public boolean createIndex (Fragment tablet, Column column) throws IOException {
		
		init(tablet , column);
		
		if ( this.schema.getColumn(column.getName()) == null ) {
			return false;
		}
		
		this.storeType = tablet.getMeta().getStoreType().getNumber();
		Fragment[] tbs = new Fragment[1];
		tbs[0] = tablet;
		switch(this.storeType){
		case StoreType.RAW_VALUE:
			this.scanner = new RawFile2.RawFileScanner(conf, this.schema, tbs);
			writeData(scanner, column);
			scanner.close();
			break;
		case StoreType.CSV_VALUE:
			this.scanner = new CSVFile2.CSVScanner(conf, this.schema, tbs);
			writeData(scanner, column);
			scanner.close();
			break;
		case StoreType.MEM_VALUE:
			break;
			default:
		}
		return true;
	}
	
	private void init (Fragment tablet, Column column) throws IOException {
		
		this.tablePath = tablet.getPath();
		this.parentPath = new Path(tablePath.getParent().getParent() , "index" );
		this.schema = tablet.getMeta().getSchema();
		this.fileName = tablet.getId() + "." + tablet.getStartOffset()
				+ "." + column.getName() +  ".index";
		Path indexPath = new Path(this.parentPath, fileName);
		
		if(fs != null) {
			fs.close();
		}
		fs = parentPath.getFileSystem(conf);
		if (!fs.exists(parentPath)) {
			fs.mkdirs(parentPath);
		}
		
		if (!fs.exists(indexPath)) {
			out = fs.create(indexPath);
		} else {
			throw new IOException ("index file is already created.");
		}
		
	}
	
	
	/**
	 * 
	 * @param scanner
	 * @param column
	 * @throws IOException
	 */
	private void writeData (FileScanner scanner , Column column) throws IOException {
		
		Tuple tuple = null;
		DataType dataType = column.getDataType();
		KeyOffsetCollector collector = new KeyOffsetCollector(dataType);
		KeyOffsetCollector rootCollector = null;
		
		/*data reading phase*/
		
		while ( true) {
			long offset = scanner.getNextOffset();
			tuple = scanner.next();
			if(tuple == null)
				break;
			for (int i = 0 ; i < schema.getColumnNum() ; i++) {
				Column col = schema.getColumn(i);
				if ( col == null ) {
					continue;
				}
				if ( dataType != col.getDataType() ) {
					continue;
				}
				if ( !col.equals(column) ) {
					continue;
				}
			
				collector.put(tuple.get(i), offset);
		
				
			}
		}
		
		/*two level initialize*/
		if ( this.level == IndexCreater.TWO_LEVEL_INDEX ) {
			rootCollector = new KeyOffsetCollector(dataType);
		}
		
		/*data writing phase*/
		TreeMap<Datum , LinkedList<Long>> keyOffsetMap = collector.getMap();
		Set<Datum> keySet = keyOffsetMap.keySet();
		
		int type = dataType.getNumber();
		int entryNum = keySet.size();
		
		//header write => type = > level => entryNum
		out.writeInt(type);
		out.writeInt(this.level);
		out.writeInt(entryNum);
		out.flush();
	
		int loadCount = this.loadNum -1;
		for (Datum key : keySet ) {
				
			if ( this.level == IndexCreater.TWO_LEVEL_INDEX ) {
				loadCount ++;
				if( loadCount == this.loadNum ) {
					rootCollector.put(key, out.getPos());
					loadCount = 0;
				}
			}
			/*key writing*/
			
			if(dataType == DataType.STRING || dataType == DataType.IPv4) {
				byte[] buf;
				buf = key.asByteArray();
				out.writeInt(buf.length);
				out.write(buf);
			} else {
				byte [] buf;
				buf = key.asByteArray();
				out.write(buf);
			}
			/**/
			
			LinkedList<Long> offsetList =  keyOffsetMap.get(key);
			/*offset num writing*/
			int offsetSize = offsetList.size();
			out.writeInt(offsetSize);
			/*offset writing*/
			for (Long offset : offsetList) {
				out.writeLong(offset);
			}
			
		}

		out.flush();
		out.close();
		keySet.clear();
		collector.clear();
		
		/*root index creating phase*/
		if (this.level == IndexCreater.TWO_LEVEL_INDEX ) {
			TreeMap<Datum, LinkedList<Long>> rootMap = rootCollector.getMap();
			keySet = rootMap.keySet();
			
			out = fs.create(new Path(this.parentPath, fileName + ".root"));
			out.writeInt(type);
			out.writeInt(this.loadNum);
			out.writeInt(keySet.size());
			
			/*root key writing*/
			for ( Datum key : keySet ) {
				
				if(dataType == DataType.STRING || dataType == DataType.IPv4) {
					byte[] buf;
					buf = key.asByteArray();
					out.writeInt(buf.length);
					out.write(buf);
				} else {
					byte [] buf;
					buf = key.asByteArray();
					out.write(buf);
				}
			
				
				LinkedList<Long> offsetList = rootMap.get(key);
				if(offsetList.size() > 1 || offsetList.size() == 0) {
					throw new IOException("Why root index doen't have one offset?");
				}
				out.writeLong(offsetList.getFirst());
				
			}
			out.flush();
			out.close();
			keySet.clear();
			rootCollector.clear();
		}
		
		
	}
	
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		scanner.close();
		out.close();
		fs.close();
		
	}
	
	
	
	/**
	 * data structure for index
	 * use treemap (to sort keys and to collect offsets)
	 * @author Ryu Hyo Seok
	 *
	 */
	private class KeyOffsetCollector {
		/**/
		private TreeMap<Datum , LinkedList<Long>> map;
		
		
		public KeyOffsetCollector ( DataType dataType ) {
			map = new TreeMap<Datum , LinkedList<Long>> ();
		}
		
		public void put (Datum key , long offset) {
			if (map.containsKey(key)) {
				map.get(key).add(offset);
			} else {
				LinkedList<Long> list = new LinkedList<Long> ();
				list.add(offset);
				map.put(key, list);
			}
		}

		public TreeMap< Datum , LinkedList<Long>> getMap () {
			return this.map;
		}
		public void clear () {
			this.map.clear();
		}
	}
	
}
