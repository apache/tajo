package nta.engine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nta.catalog.Schema;
import nta.conf.NtaConf;
import nta.engine.ipc.protocolrecords.Tablet;
import nta.storage.FileScanner;
import nta.storage.StorageManager;
import nta.storage.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ResultSetWritable implements Writable, FileScanner {

	private Path resultPath = null;
	private FileScanner scanner = null;
	private FileSystem fs = null;
	private NtaConf conf = null;
	private StorageManager sm = null;
	
	public ResultSetWritable() {
	}

	public ResultSetWritable(Path resultPath) {
		this.resultPath = resultPath;
	}

	public void setResult(Path resultPath) {
		this.resultPath = resultPath;
	}

	public Path getResults() {
		return this.resultPath;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, resultPath.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
			setResult(new Path(Text.readString(in)));
	}

	@Override
	public Tuple next() throws IOException {
		if (this.resultPath == null)
			return null;

		if (scanner == null) {
			this.conf = new NtaConf();
			fs = FileSystem.get(conf);
			sm = new StorageManager(conf, fs);
			scanner = sm.getScanner(resultPath);
		}
		return scanner.next();
	}

	@Override
	public void reset() throws IOException {
		scanner.reset();
	}

	@Override
	public void close() throws IOException {
		scanner.close();
	}

  @Override
  public void init(Configuration conf, Schema schema, Tablet[] tablets)
      throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Schema getSchema() {
    // TODO Auto-generated method stub
    return null;
  }
}
