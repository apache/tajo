package tajo.benchmark;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.net.NetUtils;
import tajo.catalog.CatalogConstants;
import tajo.catalog.Schema;
import tajo.catalog.store.MemStore;
import tajo.client.TajoClient;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class BenchmarkSet {
  protected TajoClient tajo;
  protected Map<String, Schema> schemas = new HashMap<String, Schema>();
  protected Map<String, Schema> outSchemas = new HashMap<String, Schema>();
  protected Map<String, String> queries = new HashMap<String, String>();
  protected String dataDir;

  public void init(TajoConf conf, String dataDir) throws IOException {
    this.dataDir = dataDir;
    if (System.getProperty(ConfVars.TASKRUNNER_LISTENER_ADDRESS.varname) != null) {
      tajo = new TajoClient(NetUtils.createSocketAddr(
          System.getProperty(ConfVars.TASKRUNNER_LISTENER_ADDRESS.varname)));
    } else {
      conf.set(CatalogConstants.STORE_CLASS, MemStore.class.getCanonicalName());
      tajo = new TajoClient(conf);
    }
  }

  protected void loadQueries(String dir) throws IOException {
    File queryDir = new File(dir);

    int last;
    String name, query;
    for (String file : queryDir.list()) {
      if (file.endsWith(".tql")) {
        last = file.indexOf(".tql");
        name = file.substring(0, last);
        query = FileUtil.readTextFile(new File(queryDir + "/" + file));
      }
    }
  }

  public abstract void loadSchemas();

  public abstract void loadOutSchema();

  public abstract void loadQueries() throws IOException;

  public abstract void loadTables() throws ServiceException;

  public String [] getTableNames() {
    return schemas.keySet().toArray(new String[schemas.size()]);
  }

  public String getQuery(String queryName) {
    return queries.get(queryName);
  }

  public Schema getSchema(String name) {
    return schemas.get(name);
  }

  public Iterable<Schema> getSchemas() {
    return schemas.values();
  }

  public Schema getOutSchema(String name) {
    return outSchemas.get(name);
  }

  public void perform(String queryName) throws IOException, ServiceException {
    String query = getQuery(queryName);
    if (query == null) {
      throw new IllegalArgumentException("#{queryName} does not exists");
    }
    long start = System.currentTimeMillis();
    tajo.executeQuery(query);
    long end = System.currentTimeMillis();

    System.out.println("====================================");
    System.out.println("QueryName: " + queryName);
    System.out.println("Query: " + query);
    System.out.println("Processing Time: " + (end - start));
    System.out.println("====================================");
  }
}
