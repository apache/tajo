package tajo.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.engine.ipc.protocolrecords.Fragment;

import java.io.IOException;

public abstract class SingleStorge {
  protected final Configuration conf;

  public SingleStorge(final Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public abstract Appender getAppender(TableMeta meta, Path path)
    throws IOException;

  public abstract Scanner openSingleScanner(Schema schema, Fragment fragment)
    throws IOException;
}
