package nta.engine.ipc.protocolrecords;

import java.net.URI;
import java.util.List;

import nta.engine.parser.NQL.Query;

/**
 * This contains the rewrote query and its global query plan. 
 * 
 * @author hyunsik
 */
public interface SubQueryRequest {
  public Query getQuery();

  public List<Tablet> getTablets();

  public URI getOutputDest();
}
