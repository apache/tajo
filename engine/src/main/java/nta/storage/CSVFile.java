package nta.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.conf.NtaConf;

/**
 * @author jimin
 * @author Hyunsik Choi
 *
 */
public class CSVFile implements Scanner {
	public static final String DELIMITER="csvfile.delimiter";
	
	private final Path tablePath;
	private final Path dataPath;
	private final Schema schema;
	private final FileSystem fs;
	
	private FileStatus[] filelist;
	private BufferedReader in;
	private int currentFileIdx;
	private String line;
	private String delimeter;
	private MemTuple readTuple;
	
	
	public CSVFile(NtaConf conf, Store store) throws IOException {
		this.tablePath = new Path(store.getURI());
		this.schema = store.getSchema();		
		this.fs = tablePath.getFileSystem(conf);
		
		this.dataPath = new Path(tablePath, "data");
		this.delimeter = store.getOption(DELIMITER);
		this.currentFileIdx = 0;
		this.filelist = null;
	}
	
	@Override
	public void init() throws IOException {
		if(fs.isDirectory(this.dataPath)) {
			filelist = fs.listStatus(this.dataPath);
			FSDataInputStream fis = fs.open(filelist[currentFileIdx++].getPath());
			InputStreamReader r = new InputStreamReader(fis);
			in = new BufferedReader(r);
		} else {
			FSDataInputStream fis = fs.open(this.dataPath);
			InputStreamReader r = new InputStreamReader(fis);
			in = new BufferedReader(r);
		}		
	}

	/* (non-Javadoc)
	 * @see nta.query.executor.ScanExec#hasNextTuple()
	 */
	@Override
	public Tuple next() throws IOException {		
		if((this.line = in.readLine()) == null)
			return null;

		this.readTuple = new MemTuple();
		String[] cells = null;

		cells = line.split(delimeter);
		Column field;
		for(int i=0;i< cells.length;i++) {
			field = schema.getColumn(i);
			switch(field.getDataType()) {
			case LONG :
				this.readTuple.putLong(i, Long.valueOf(cells[i]));
				break;
			case INT :
				this.readTuple.putInt(i, Integer.valueOf(cells[i]));
				break;
			case SHORT :
				this.readTuple.putShort(i, Short.valueOf(cells[i]));
				break;
			case IPv4 :
				this.readTuple.putIPv4(i, Inet4Address.getByName(cells[i]));
				break;
			}
		}

		return this.readTuple;
	}	
	
	@Override
	public VTuple next2() throws IOException {
		if((this.line = in.readLine()) == null)
			return null;

		VTuple tuple = new VTuple(schema.getColumnNum());
		String[] cells = null;

		cells = line.split(delimeter);
		Column field;
		for(int i=0;i< cells.length;i++) {
			field = schema.getColumn(i);
			String cell = cells[i].trim();
			switch(field.getDataType()) {
			case SHORT :
				tuple.put(i, Short.valueOf(cell));
				break;
			case INT :
				tuple.put(i, Integer.valueOf(cell));
				break;
			case LONG :
				tuple.put(i, Long.valueOf(cell));
				break;
			case FLOAT :
				tuple.put(i, Float.valueOf(cell));
				break;
			case DOUBLE :
				tuple.put(i, Double.valueOf(cell));
				break;
			case STRING :
				tuple.put(i, cell);
				break;
			case IPv4 :
				if(cells[i].charAt(0) == '/') {
					tuple.put(i, Inet4Address.getByName(cells[i].substring(1, 
						cell.length())));
				}	
				break;
			}
		}

		return tuple;
	}

	@Override
	public void reset() throws IOException {
		// TODO - NTA-133
	}

	////////////////////////////////////////////////////////////////
	// Storage Implementation
	////////////////////////////////////////////////////////////////
	@Override
	public boolean isLocalFile() {
		return true;
	}

	@Override
	public boolean readOnly() {
		return false;
	}

	@Override
	public boolean canRandomAccess() {
		return false;
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public Schema getSchema() {
		return schema;
	}
}