package tajo.storage;

import org.apache.hadoop.conf.Configuration;
import tajo.catalog.Schema;
import tajo.engine.ipc.protocolrecords.Fragment;

public abstract class FileScanner implements SeekableScanner {
  
  protected final Configuration conf;
  protected final Schema schema;
  protected final Fragment[] tablets;
  
  public FileScanner(Configuration conf, final Schema schema, 
      final Fragment [] tablets) {
    this.conf = conf;
    this.schema = schema;
    this.tablets = tablets;
  }

  @Override
	public Schema getSchema() {
	  return schema;
	}
}
