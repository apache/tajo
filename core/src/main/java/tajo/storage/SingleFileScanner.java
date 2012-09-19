package tajo.storage;

import org.apache.hadoop.conf.Configuration;
import tajo.catalog.Schema;
import tajo.ipc.protocolrecords.Fragment;

import java.io.IOException;

public abstract class SingleFileScanner implements Scanner{
  
  protected final Configuration conf;
  protected final Schema schema;
  protected final Fragment fragment;
  
  public SingleFileScanner(Configuration conf, final Schema schema, 
      final Fragment fragment) {
    this.conf = conf;
    this.schema = schema;
    this.fragment = fragment;
  }

  public abstract void seek(long offset) throws IOException;
  public abstract long getNextOffset() throws IOException;
  public abstract long getPos() throws IOException;

  @Override
  public Schema getSchema() {
    return schema;
  }
}
