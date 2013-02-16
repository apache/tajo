package tajo.storage;

import org.apache.hadoop.conf.Configuration;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;

import java.io.IOException;

public abstract class FileScanner implements Scanner {
  protected boolean inited = false;
  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  protected final Fragment fragment;

  protected Column [] targets;
  
  public FileScanner(Configuration conf, final TableMeta meta, final Fragment fragment) {
    this.conf = conf;
    this.meta = meta;
    this.schema = meta.getSchema();
    this.fragment = fragment;
  }

  public void init() throws IOException {
    inited = true;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void setTarget(Column[] targets) {
    if (inited) {
      throw new IllegalStateException("Should be called before init()");
    }
    this.targets = targets;
  }

  public void setSearchCondition(Object expr) {
    if (inited) {
      throw new IllegalStateException("Should be called before init()");
    }
  }
}
