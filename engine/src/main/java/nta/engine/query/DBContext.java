package nta.engine.query;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import nta.catalog.CatalogServer;
import nta.conf.NtaConf;
import nta.storage.StorageManager;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class DBContext {
	private final NtaConf conf;
	private final CatalogServer catalog;
	private final StorageManager storageMamager;
	private PrintStream output = null;	
	
	public DBContext(NtaConf conf, StorageManager storageManager, CatalogServer catlog) throws IOException, URISyntaxException {
		this.conf = conf;
		this.storageMamager = storageManager;
		this.catalog = catlog;
	}
	
	public CatalogServer getCatalog() {
		return this.catalog;
	}
	
	public StorageManager getStorageManager() {
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
