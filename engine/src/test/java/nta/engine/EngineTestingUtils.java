/**
 * 
 */
package nta.engine;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.datum.DatumFactory;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.StorageUtil;
import nta.storage.Tuple;
import nta.storage.VTuple;
import nta.util.FileUtil;
import nta.util.ReflectionUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;

/**
 * @author Hyunsik Choi
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
	
	public final static Schema mockupSchema;
	public final static TableMeta mockupMeta;
	
	static {
	mockupSchema = new Schema();
	mockupSchema.addColumn("deptname", DataType.STRING);
	mockupSchema.addColumn("score", DataType.INT);
  mockupMeta = TCatUtil.newTableMeta(mockupSchema, StoreType.CSV);
	}
	
	public static void writeTmpTable(Configuration conf, String parent, 
	    String tbName, boolean writeMeta) throws IOException {
	  StorageManager sm = StorageManager.get(conf, parent);

    Appender appender = null;
    if (writeMeta) {
      appender = sm.getTableAppender(mockupMeta, tbName);
    } else {
      FileSystem fs = sm.getFileSystem();
      fs.mkdirs(StorageUtil.concatPath(parent, tbName, "data"));
      appender = sm.getAppender(mockupMeta, 
          StorageUtil.concatPath(parent, tbName, "data", "tb000"));
    }
    int deptSize = 10000;
    int tupleNum = 100;
    Tuple tuple = null;
    for (int i = 0; i < tupleNum; i++) {
      tuple = new VTuple(2);
      String key = "test" + (i % deptSize);
      tuple.put(0, DatumFactory.createString(key));
      tuple.put(1, DatumFactory.createInt(i + 1));
      appender.addTuple(tuple);
    }
    appender.close();
	}
}
