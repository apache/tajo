/**
 * 
 */
package tajo.engine.ipc;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.zookeeper.KeeperException;

/**
 * @author jimin
 * 
 */
public interface QueryEngineInterface extends VersionedProtocol {
	public long versionId = 0;
  String executeQuery(String query) throws Exception;

  String executeQueryAsync(String query);

  void attachTable(String name, String strPath) throws Exception;

  void detachTable(String name) throws Exception;

  boolean existsTable(String name);
  
  public String getClusterInfo() throws KeeperException, InterruptedException;
  
  public String getTableList();
  
  public String getTableDesc(String name);
}
