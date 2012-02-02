package nta.storage;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.conf.NtaConf;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.ipc.protocolrecords.Fragment;

public class IndexScanner {
	
	private FileSystem fs;
	private FSDataInputStream indexIn;
	private FSDataInputStream subIn;
	
	private int level;
	private int entryNum;
	private int loadNum = -1;
	private DataType dataType;
	private Path tablePath;
	private Path parentPath;
	private String fileName;
	private Column indexColumn;
	
	/*functions*/
	/**
	 * 
	 * @param conf
	 * @param tablets
	 * @throws IOException
	 */
	public IndexScanner(NtaConf conf, final Fragment tablets)
			throws IOException {
		init(conf, tablets.getMeta().getSchema(), tablets, null);
	}

	/**
	 * 
	 * @param conf
	 * @param tablets
	 * @param column
	 * @throws IOException
	 */
	public IndexScanner(NtaConf conf , final Fragment tablets , Column column) 
			throws IOException {
		init(conf, tablets.getMeta().getSchema(), tablets, column);
	}
	
	private void init(NtaConf conf, final Schema schema, final Fragment tablets, Column column) 
			throws IOException {
		
		this.tablePath = tablets.getPath();
		this.parentPath = new Path(tablePath.getParent().getParent() , "index");
		
		
		fs = tablePath.getFileSystem(conf);
		if(!fs.exists(tablePath)) {
			throw new FileNotFoundException("data file did not created");
		}
		fs.close();
		
		/*index file access stream*/
		fs = parentPath.getFileSystem(conf);
		if(!fs.exists(parentPath)) {
			throw new FileNotFoundException("index did not created");
		}
		Column col = column;
		if(col == null) {
			for (int i = 0 ; i < schema.getColumnNum() ; i ++ ) {
				col = schema.getColumn(i);
				this.fileName =  tablets.getId() + "." + tablets.getStartOffset()
						+ "." + col.getId() + "." + col.getName() +  ".index";
				if(fs.exists(new Path(parentPath, this.fileName ))) {
					break;
				}
			}
		} else {
			this.fileName = tablets.getId() + "." + tablets.getStartOffset()
					+ "." + col.getId() + "." + col.getName() +  ".index";
		}
		indexIn = fs.open(new Path(parentPath, this.fileName));
		/*read header*/
		/*type => level => entrynum*/
		this.dataType = DataType.valueOf(indexIn.readInt());
		
		if(this.dataType != col.getDataType()) {
			throw new IOException("datatype is different");
		}
		
		this.level = indexIn.readInt();
		this.entryNum = indexIn.readInt();
		this.indexColumn = col;
		
		fillData();
		
	}
	private void fillData() throws IOException {
		
		/*load on memory*/
		if( this.level == IndexCreater.TWO_LEVEL_INDEX ) {
			
			Path rootPath = new Path( parentPath, this.fileName + ".root" );
			if( !fs.exists(rootPath) ) {
				throw new FileNotFoundException("root index did not created");
			}
			
			subIn = indexIn;
			indexIn = fs.open(rootPath);
			/*root index header reading : type => loadNum => indexSize */
			indexIn.readInt();
			this.loadNum = indexIn.readInt();
			this.entryNum = indexIn.readInt();
			/**/
			fillRootIndex(entryNum , indexIn);
			
		} else {
			fillLeafIndex (entryNum , indexIn , -1);
		}		
	}
	
	/**
	 * 
	 * @param offset
	 * @return
	 * @throws IOException 
	 */
	public long[] seekEQL(Datum key) throws IOException {
		
		int pos = -1;
		switch (this.level) {
		case IndexCreater.ONE_LEVEL_INDEX:
			pos = oneLevBS(key);
			break;
		case IndexCreater.TWO_LEVEL_INDEX:
			pos = twoLevBS(key , this.loadNum);
			break;
		}
		
		if(!correctable){
			return null;
		}else
			return this.offsetSubIndex[pos];
	}
	public long[] seekGTH(Datum key) throws IOException {
		int pos = -1;
		switch(this.level) {
		case IndexCreater.ONE_LEVEL_INDEX:
			pos = oneLevBS(key);
			break;
		case IndexCreater.TWO_LEVEL_INDEX:
			pos = twoLevBS(key , this.loadNum + 1);
			break;
		}
		if( pos + 1 >= this.offsetSubIndex.length ) {
			return null;
		}
		return this.offsetSubIndex[pos + 1];
	}
	
	
	private void fillLeafIndex(int entryNum , FSDataInputStream in , long pos) 
			throws IOException{
		int counter = 0;
		try {
			if(pos != -1) {
				in.seek(pos);
			}
			this.dataSubIndex = new Datum[entryNum];
			this.offsetSubIndex = new long[entryNum][];
			
			for(int i = 0 ; i < entryNum ; i ++) {
				counter ++;
				switch(this.dataType) {
				case BYTE:
					dataSubIndex[i] = DatumFactory.createByte(in.readByte());
					break;
				case SHORT:
					dataSubIndex[i] = DatumFactory.createShort(in.readShort());
					break;
				case INT:
					dataSubIndex[i] = DatumFactory.createInt(in.readInt());
					break;
				case LONG:
					dataSubIndex[i] = DatumFactory.createLong(in.readLong());
					break;
				case FLOAT:
					dataSubIndex[i] = DatumFactory.createFloat(in.readFloat());
					break;
				case DOUBLE:
					dataSubIndex[i] = DatumFactory.createDouble(in.readDouble());
					break;
				case STRING:
					int len = in.readInt();
					byte[] buf = new byte[len];
					in.read(buf);
					dataSubIndex[i] = DatumFactory.createString(new String(buf));
					break;
				case IPv4:
					len = in.readInt();
					buf = new byte[len];
					in.read(buf);
					dataSubIndex[i] = DatumFactory.createIPv4(buf);
					break;
				}
				
				int offsetNum = in.readInt();
				this.offsetSubIndex[i] = new long[offsetNum];
				for(int j = 0; j < offsetNum ; j ++ ){
					this.offsetSubIndex[i][j] = in.readLong();
				}
				
			}
			
		}catch (IOException e) {
			counter --;
			if(pos != -1) {
				in.seek(pos);
			}
			this.dataSubIndex = new Datum[counter];
			this.offsetSubIndex = new long[counter][];
			
			for(int i = 0 ; i < counter ; i ++) {
				switch(this.dataType) {
				case BYTE:
					dataSubIndex[i] = DatumFactory.createByte(in.readByte());
					break;
				case SHORT:
					dataSubIndex[i] = DatumFactory.createShort(in.readShort());
					break;
				case INT:
					dataSubIndex[i] = DatumFactory.createInt(in.readInt());
					break;
				case LONG:
					dataSubIndex[i] = DatumFactory.createLong(in.readLong());
					break;
				case FLOAT:
					dataSubIndex[i] = DatumFactory.createFloat(in.readFloat());
					break;
				case DOUBLE:
					dataSubIndex[i] = DatumFactory.createDouble(in.readDouble());
					break;
				case STRING:
					int len = in.readInt();
					byte[] buf = new byte[len];
					in.read(buf);
					dataSubIndex[i] = DatumFactory.createString(new String(buf));
					break;
				case IPv4:
					len = in.readInt();
					buf = new byte[len];
					in.read(buf);
					dataSubIndex[i] = DatumFactory.createIPv4(buf);
					break;
				}
				int offsetNum = in.readInt();
				this.offsetSubIndex[i] = new long[offsetNum];
				for(int j = 0; j < offsetNum ; j ++ ){
					this.offsetSubIndex[i][j] = in.readLong();
				}
				
			}
		}
	}
	
	
	
	private void fillRootIndex(int entryNum, FSDataInputStream in) 
			throws IOException{
		this.dataIndex = new Datum[entryNum];
		this.offsetIndex = new long[entryNum];
		
		for(int i = 0 ; i < entryNum ; i ++ ) {
			switch(this.dataType) {
			case BYTE:
				dataIndex[i] = DatumFactory.createByte(in.readByte());
				break;
			case SHORT:
				dataIndex[i] = DatumFactory.createShort(in.readShort());
				break;
			case INT:
				dataIndex[i] = DatumFactory.createInt(in.readInt());
				break;
			case LONG:
				dataIndex[i] = DatumFactory.createLong(in.readLong());
				break;
			case FLOAT:
				dataIndex[i] = DatumFactory.createFloat(in.readFloat());
				break;
			case DOUBLE:
				dataIndex[i] = DatumFactory.createDouble(in.readDouble());
				break;
			case STRING:
				int len = in.readInt();
				byte[] buf = new byte[len];
				in.read(buf);
				dataIndex[i] = DatumFactory.createString(new String(buf));
				break;
			case IPv4:
				len = in.readInt();
				buf = new byte[len];
				in.read(buf);
				dataIndex[i] = DatumFactory.createIPv4(buf);
				break;
			}
			this.offsetIndex[i] = in.readLong();
			
		}
		
	}
	
		/*memory index, only one is used.*/
		private Datum[] dataIndex = null;
		private Datum[] dataSubIndex = null;
		
		/*offset index*/
		private long[] offsetIndex = null;
		private long[][] offsetSubIndex = null;
		
		
		private boolean correctable = true;
		
		private int oneLevBS (Datum key) throws IOException {
			int pos = -1;
			
			correctable = true;
			pos = binarySearch(this.dataSubIndex , key , 0 ,this.dataSubIndex.length);
			return pos;
		}
		private int twoLevBS (Datum key , int loadNum) throws IOException{
			int pos = -1;
			
			pos = binarySearch(this.dataIndex , key, 0 , this.dataIndex.length);
			fillLeafIndex(loadNum , subIn , this.offsetIndex[pos]);
			pos = binarySearch(this.dataSubIndex, key , 0 , this.dataSubIndex.length );
			
			return pos;
		}
		
		
		private int binarySearch (Datum[] arr, Datum key, int startPos, int endPos) {
			int offset = -1;
			int start = startPos;
			int end = endPos;
			int centerPos = (start + end)/2;
			while(true){
				if(arr[centerPos].compareTo(key) > 0 ) {
					if(centerPos == 0) {
						correctable = false;
						break;
					} else if(arr[centerPos -1].compareTo(key) < 0 ) {
						correctable = false;
						offset = centerPos -1;
						break;
					} else {
						end = centerPos;
						centerPos = (start + end)/2;
					}
				} else if(arr[centerPos].compareTo(key) < 0) {
					if(centerPos == arr.length -1){
						correctable = false;
						offset = centerPos;
						break;
					} else if(arr[centerPos + 1].compareTo(key) > 0) {
						correctable = false;
						offset = centerPos;
						break;
					} else {
						start = centerPos + 1;
						centerPos = (start + end)/2;
					}
				} else {
					correctable = true;
					offset =  centerPos;
					break;
				}
			}
			return offset;
		}
	
		
	public Column getIndexColumn () {
		return this.indexColumn;
	}
}
