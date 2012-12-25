package tajo.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;

import java.io.IOException;

public abstract class SingleFileAppender implements Appender{
  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  
  public SingleFileAppender(Configuration conf, TableMeta meta, Path path) {
    this.conf = conf;
    this.meta = meta;
    this.schema = meta.getSchema();
  }

  public abstract long getOffset() throws IOException;
}
