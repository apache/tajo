/**
 * 
 */
package nta.engine.ipc;

import nta.catalog.TableMeta;
import nta.engine.QueryResponse;
import nta.engine.ResultSetWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author jimin
 * 
 */
public interface QueryEngineInterface extends VersionedProtocol{
	public long versionID = 0;
	ResultSetWritable executeQuery(String query);

	QueryResponse executeQueryAsync(String query);

	void createTable(TableMeta meta);

	void dropTable(String name);

	void attachTable(String name, Path path);

	void detachTable(String name);

	boolean existsTable(String name);
}
