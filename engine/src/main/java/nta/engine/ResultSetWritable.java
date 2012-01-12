package nta.engine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nta.catalog.Schema;
import nta.conf.NtaConf;
import nta.storage.Scanner;
import nta.storage.StorageManager;
import nta.storage.Tuple;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ResultSetWritable implements Scanner, Writable {

	private Path resultPath = null;
	private Scanner scanner = null;
	private NtaConf conf = null;
	private StorageManager sm = null;
	
	public ResultSetWritable() {
	  this.conf = new NtaConf();
	  try {
      sm = new StorageManager(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
	}

	public ResultSetWritable(Path resultPath) {
		this();
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
			
			scanner = sm.getTableScanner(resultPath);
		}
		return scanner.next();
	}

	@Override
	public void reset() throws IOException {
		scanner.reset();
	}
	
	public void close() throws IOException {
		scanner.close();
	}

  @Override
  public Schema getSchema() {
    // TODO Auto-generated method stub
    return null;
  }
}
