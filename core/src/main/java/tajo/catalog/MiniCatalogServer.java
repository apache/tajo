/**
 * 
 */
package tajo.catalog;

import tajo.conf.TajoConf;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 *
 */
public class MiniCatalogServer {
  private CatalogServer catalogServers;
  
  public MiniCatalogServer(TajoConf conf) throws IOException {
    this.catalogServers = new CatalogServer(conf);
    this.catalogServers.start();
  }
  
  public void shutdown() {
    this.catalogServers.shutdown("Normally shuting down");
  }
  
  public CatalogServer getCatalogServer() {
    return this.catalogServers;
  }
  
  public CatalogService getCatalog() {
    return new LocalCatalog(this.catalogServers);
  }
}
