package nta.engine.query;

import java.net.URI;
import java.util.List;

import nta.engine.ipc.protocolrecords.SubQueryRequest;
import nta.engine.ipc.protocolrecords.Tablet;

/**
 * @author hyunsik
 * 
 */
public class SubQueryRequestImpl implements SubQueryRequest {  
  private final List<Tablet> tablets;
  private final URI dest;
  private final String query;
  private final String tableName;
  /**
	 * 
	 */
public SubQueryRequestImpl(List<Tablet> tablets, URI output, String query, String tableName) {
    this.tablets = tablets;
    this.dest = output;
    this.query = query;
    this.tableName = tableName;
  }

  @Override
  public String getQuery() {
    return this.query;
  }

  @Override
  public List<Tablet> getTablets() {
    return this.tablets;
  }

  @Override
  public URI getOutputDest() {
    return this.dest;
  }
  
  public String getTableName() {
    return this.tableName;
  }
}
