package nta.engine;

import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.exception.NoSuchTableException;
import nta.engine.exception.NTAQueryException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.VersionedProtocol;

@Deprecated
public interface NtaEngineInterface extends VersionedProtocol {
	public void createTable(TableDesc meta) throws Exception;
	public void dropTable(String name) throws Exception;
	public void attachTable(String name, Path path) throws Exception;
	public void detachTable(String name) throws Exception;
	public String executeQueryC(String query) throws NTAQueryException;
	public void updateQuery(String query) throws NTAQueryException;
	public TableDesc getTableDesc(String name) throws NoSuchTableException;
	public boolean existsTable(String name);
}
