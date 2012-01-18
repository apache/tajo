package nta.storage;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.ipc.protocolrecords.Fragment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public abstract class Storage {
  protected final Configuration conf;
  
  public Storage(final Configuration conf) {
    this.conf = conf;
  }
  
  public Configuration getConf() {
    return this.conf;
  }
  
  public abstract Appender getAppender(Schema schema, Path path)
    throws IOException;
  
  public abstract Scanner openScanner(Schema schema,
      Fragment [] tablets) throws IOException;
}
