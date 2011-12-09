package nta.engine.query;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import nta.catalog.Catalog;
import nta.conf.NtaConf;
import nta.storage.StoreManager;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class DBContext {
	private final NtaConf conf;
	private final Catalog catalog;
	private final StoreManager storageMamager;
	private PrintStream output = null;	
	
	public DBContext(NtaConf conf, StoreManager storageManager, Catalog catlog) throws IOException, URISyntaxException {
		this.conf = conf;
		this.storageMamager = storageManager;
		this.catalog = catlog;
	}
	
	public Catalog getCatalog() {
		return this.catalog;
	}
	
	public StoreManager getStorageManager() {
		return this.storageMamager;
	}
	
	public NtaConf getConf() {
		return this.conf;
	}
	
	public boolean hasPrintStream() {
		return this.output != null;
	}
	
	public void setPrintStream(PrintStream out) {
		this.output = out;
	}
	
	public PrintStream getPrintStream() {
		return this.output;
	}
}
