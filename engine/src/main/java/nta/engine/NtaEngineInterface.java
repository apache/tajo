package nta.engine;

import nta.catalog.TableInfo;
import nta.catalog.TableMeta;
import nta.catalog.exception.NoSuchTableException;
import nta.engine.exception.NTAQueryException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface NtaEngineInterface extends VersionedProtocol {
	public void createTable(TableMeta meta) throws Exception;
	public void dropTable(String name) throws Exception;
	public void attachTable(String name, Path path) throws Exception;
	public void detachTable(String name) throws Exception;
	public String executeQueryC(String query) throws NTAQueryException;
	public void updateQuery(String query) throws NTAQueryException;
	public TableInfo getTableInfo(String name) throws NoSuchTableException;
	public TableInfo [] getTablesByOrder() throws Exception;
	public boolean existsTable(String name);
}
