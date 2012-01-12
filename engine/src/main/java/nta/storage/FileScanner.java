package nta.storage;

import nta.catalog.Schema;
import nta.engine.ipc.protocolrecords.Tablet;

import org.apache.hadoop.conf.Configuration;

public abstract class FileScanner implements Scanner {
  
  protected final Configuration conf;
  protected final Schema schema;
  protected final Tablet [] tablets;
  
  public FileScanner(Configuration conf, final Schema schema, 
      final Tablet [] tablets) {
    this.conf = conf;
    this.schema = schema;
    this.tablets = tablets;
  }

  @Override
	public Schema getSchema() {
	  return schema;
	}
}
