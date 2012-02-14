package nta.storage;

import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TableMeta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public abstract class FileAppender implements Appender {
  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  protected final Path path;
  protected Options option = null;
  
  public FileAppender(Configuration conf, TableMeta meta, Path path) {
    this.conf = conf;
    this.meta = meta;
    this.schema = meta.getSchema();
    this.path = path;
    this.option = meta.getOptions();
  }
}
