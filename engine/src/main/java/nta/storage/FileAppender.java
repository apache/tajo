package nta.storage;

import nta.catalog.Options;
import nta.catalog.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public abstract class FileAppender implements Appender {
  protected final Configuration conf;
  protected final Schema schema;
  protected final Path path;
  protected Options option = null;
  
  public FileAppender(Configuration conf, Schema schema, Path path) {
    this.conf = conf;
    this.schema = schema;
    this.path = path;
  }
  
  public FileAppender(Configuration conf, Schema schema, Path path, Options option) {
    this(conf, schema, path);
    this.option = option;
  }
}
