/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nta.catalog.Schema;
import nta.engine.QueryIdFactory;
import nta.engine.QueryUnitId;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.CreateTableNode;
import nta.engine.planner.logical.UnaryNode;

/**
 * @author jihoon
 *
 */
public class QueryUnit {

	private QueryUnitId id;
	private LogicalNode op = null;
	private CreateTableNode store = null;
	private UnaryNode mid = null;
	private ScanNode scan = null;
	private List<Fragment> fragments;
	private Set<QueryUnit> nextQueries;
	private Set<QueryUnit> prevQueries;
	
	private String hostName;
	private int port;
	
	public QueryUnit(QueryUnitId id) {
		this.id = id;
		nextQueries = new HashSet<QueryUnit>();
		prevQueries = new HashSet<QueryUnit>();
		fragments = new ArrayList<Fragment>();
	}
	
	public QueryUnit setStoreNode(CreateTableNode store) {
	  try {
	    this.store = (CreateTableNode) store.clone();
      return this;
    } catch (CloneNotSupportedException e) {
      e.printStackTrace();
    }
	  return null;
	}
	
	public QueryUnit setUnaryNode(UnaryNode node) {
	  try {
      this.mid = (UnaryNode) node.clone();
      return this;
    } catch (CloneNotSupportedException e) {
      e.printStackTrace();
    }
    return null;
	}
	
	public QueryUnit setScanNode(ScanNode scan) {
	  try {
      this.scan = (ScanNode) scan.clone();
      return this;
    } catch (CloneNotSupportedException e) {
      e.printStackTrace();
    }
	  return null;
	}
	
	public LogicalNode buildLogicalPlan() {
	  if (this.op == null) {
	    if (this.mid != null) {
        this.mid.setSubNode(this.scan);
	      this.store.setSubNode(this.mid);
	    } else {
	      this.store.setSubNode(this.scan);
	    }
	    this.op = this.store;
	  }
	  return this.op;
	}
	
	public void setHost(String host, int port) {
		this.hostName = host;
		this.port = port;
	}
	
	public void addFragment(Fragment fragment) {
	  fragment.setId(this.scan.getTableId());
		this.fragments.add(fragment);
	}
	
	public void addFragments(Fragment[] fragments) {
	  for (Fragment frag : fragments) {
      this.addFragment(frag);
    }
	}
	
	public void setFragments(Fragment[] fragments) {
	  this.fragments.clear();
	  this.addFragments(fragments);
	}
	
	public void addNextQuery(QueryUnit query) {
		nextQueries.add(query);
	}
	
	public void addPrevQuery(QueryUnit query) {
		prevQueries.add(query);
	}
	
	public void removeNextQuery(QueryUnit query) {
		nextQueries.remove(query);
	}
	
	public void removePrevQuery(QueryUnit query) {
		prevQueries.remove(query);
	}
	
	public List<Fragment> getFragments() {
		return this.fragments;
	}
	
	public Set<QueryUnit> getNextQueries() {
		return this.nextQueries;
	}
	
	public Set<QueryUnit> getPrevQueries() {
		return this.prevQueries;
	}
	
	public QueryUnitId getId() {
		return id;
	}
	
	public String getHost() {
		return this.hostName;
	}
	
	public int getPort() {
		return this.port;
	}
	
	public String getInputName() {
		return this.scan.getTableId();
	}
	
	public String getOutputName() {
		return this.store.getTableName();
	}
	
	public Schema getInputSchema() {
	  return this.scan.getInputSchema();
	}
	
	public Schema getOutputSchema() {
	  return this.store.getOutputSchema();
	}
	
	public CreateTableNode getStoreTableNode() {
	  return this.store;
	}
	
	public UnaryNode getUnaryNode() {
	  return this.mid;
	}
	
	public ScanNode getScanNode() {
	  return this.scan;
	}
	
	@Override
	public String toString() {
		String str = new String(op.getType() + " ");
		for (Fragment t : fragments) {
			str += t + " ";
		}
		return str;
	}
	
	public QueryUnit cloneExceptFragments() {
	  QueryUnit clone = new QueryUnit(QueryIdFactory.newQueryUnitId());
	  clone.setStoreNode(this.store);
	  clone.setScanNode(this.scan);
	  if (this.mid != null) {
	    clone.setUnaryNode(this.mid);
	  }
	  clone.buildLogicalPlan();
	  clone.setHost(hostName, port);
	  for (QueryUnit next : this.nextQueries) {
	    clone.addNextQuery(next);
	  }
	  for (QueryUnit prev : this.prevQueries) {
	    clone.addPrevQuery(prev);
	  }

	  return clone;
	}
}