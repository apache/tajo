/**
 * 
 */
package nta.engine.ipc;

import nta.catalog.TableMeta;
import nta.engine.QueryResponse;
import nta.engine.ResultSet;

import org.apache.hadoop.fs.Path;

/**
 * @author jimin
 * 
 */
public interface QueryEngineInterface {
	ResultSet executeQuery(String query);

	QueryResponse executeQueryAsync(String query);

	void createTable(TableMeta meta);

	void dropTable(String name);

	void attachTable(String name, Path path);

	void detachTable(String name);

	boolean existsTable(String name);
}
