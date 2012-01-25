/**
 * 
 */
package nta.engine.ipc;

import nta.catalog.TableDescImpl;

import org.apache.hadoop.fs.Path;

/**
 * @author jimin
 * 
 */
public interface QueryEngineInterface {
  String executeQuery(String query);

  String executeQueryAsync(String query);

  @Deprecated
  void createTable(TableDescImpl meta);

  @Deprecated
  void dropTable(String name);

  @Deprecated
  void attachTable(String name, Path path) throws Exception;

  void attachTable(String name, String strPath) throws Exception;

  void detachTable(String name) throws Exception;

  boolean existsTable(String name);
}
