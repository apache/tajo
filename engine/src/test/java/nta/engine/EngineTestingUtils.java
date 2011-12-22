/**
 * 
 */
package nta.engine;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import nta.catalog.TableMeta;
import nta.conf.NtaConf;
import nta.storage.StorageManager;
import nta.storage.Store;
import nta.storage.UpdatableScanner;
import nta.storage.VTuple;
import nta.util.FileUtil;
import nta.util.ReflectionUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;

/**
 * @author hyunsik
 *
 */
public class EngineTestingUtils {	
	
	public static final void buildTestDir(String dir) throws IOException {
		Path path = new Path(dir);
		FileSystem fs = path.getFileSystem(new Configuration());		
		if(fs.exists(path))
			fs.delete(path, true);
		
		fs.mkdirs(path);
	}
	
	public static final void cleanTestDir(String dir) throws IOException {
		Path path = new Path(dir);
		FileSystem fs = path.getFileSystem(new Configuration());		
		if(fs.exists(path))
			fs.delete(path, true);
	}
	public static final void writeCSVTable(String tablePath, TableMeta meta, String [] tuples) 
		throws IOException {		
		File tableRoot = new File(tablePath);
		tableRoot.mkdir();
		
		File metaFile = new File(tableRoot+"/.meta");
		FileUtil.writeProto(metaFile, meta.getProto());
		
		File dataDir = new File(tableRoot+"/data");
		dataDir.mkdir();
		
		FileWriter writer = new FileWriter(
			new File(tableRoot+"/data/table1.csv"));
		for(String tuple : tuples) {
			writer.write(tuple+"\n");
		}
		writer.close();
	}
	
	public static final void writeRawTable(NtaConf conf, TableMeta meta, VTuple [] tuples) throws IOException {
//		File tableRoot = new File(tablePath);
//		tableRoot.mkdir();
		
//		File dataDir = new File(tableRoot+"/data");
//		dataDir.mkdir();
		
		FileSystem fs = LocalFileSystem.get(conf);
		StorageManager sm = new StorageManager(conf, fs);
		Store store = sm.create(meta);
		//RawFile rf = new RawFile(conf, store);
		//rf.init();
		UpdatableScanner scanner = sm.getUpdatableScanner(store);

		for (VTuple tuple : tuples) {
			scanner.addTuple(tuple);
		}
		scanner.close();

//		File metaFile = new File(tablePath+"/.meta");
//		FileUtils.writeProto(metaFile, meta.getProto());
	}
	
	/** Utility method for testing writables. */
	public static Writable testWritable(Writable before) throws Exception {
		DataOutputBuffer dob = new DataOutputBuffer();
		before.write(dob);

		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(dob.getData(), dob.getLength());

		Writable after = (Writable)ReflectionUtil.newInstance(before.getClass());
		after.readFields(dib);

		assertEquals(before, after);
		return after;
	}
}
