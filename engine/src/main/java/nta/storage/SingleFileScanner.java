package nta.storage;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.ipc.protocolrecords.Fragment;

import org.apache.hadoop.conf.Configuration;

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
  @Override
  public Schema getSchema() {
    return schema;
  }
}
