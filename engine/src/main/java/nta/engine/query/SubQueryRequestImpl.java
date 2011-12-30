package nta.engine.query;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.Path;

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

  /**
	 * 
	 */
  public SubQueryRequestImpl(List<Tablet> tablets, URI output, String query) {
    this.tablets = tablets;
    this.dest = output;
    this.query = query;
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
}
