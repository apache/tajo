package nta.engine.ipc.protocolrecords;

import java.net.URI;
import java.util.List;

/**
 * This contains the rewrote query and its global query plan. 
 * 
 * @author hyunsik
 */
public interface SubQueryRequest {
	public String getQuery();
	
	public String getTableName();

  public List<Tablet> getTablets();

  public URI getOutputDest();
}
