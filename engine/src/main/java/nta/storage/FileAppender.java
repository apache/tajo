package nta.storage;

import nta.catalog.Schema;
import nta.catalog.TableMeta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public abstract class FileAppender implements Appender {
  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  protected final Path path;
  
  public FileAppender(Configuration conf, TableMeta meta, Path path) {
    this.conf = conf;
    this.meta = meta;
    this.schema = meta.getSchema();
    this.path = path;
  }

  public abstract long getOffset() throws IOException;
}
