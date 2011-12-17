/**
 * 
 */
package nta.engine.ipc;

import org.apache.hadoop.fs.Path;

import nta.catalog.TableMeta;

/**
 * @author jimin
 *
 */
public interface QueryRequest {
	public void updateQuery(String query, boolean async);
	public void executeQuery(String query, boolean async);
	
	public void createTable(TableMeta meta);
	public void dropTable(String name);
	public void attachTable(String name, Path path);
	public void detachTable(String name);
	public boolean existsTable(String name);
}
