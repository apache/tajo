package nta.storage;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.engine.ipc.protocolrecords.Fragment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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
