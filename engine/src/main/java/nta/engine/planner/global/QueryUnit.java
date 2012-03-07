/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.List;

import nta.catalog.Schema;
import nta.engine.QueryIdFactory;
import nta.engine.QueryUnitId;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.CreateTableNode;
import nta.engine.planner.logical.UnaryNode;

import com.google.common.base.Preconditions;

/**
 * @author jihoon
 *
 */
public class QueryUnit {

	private QueryUnitId id;
	private CreateTableNode store = null;
	private LogicalNode plan = null;
	private ScanNode[] scan;
	private List<Fragment> fragments;
	
	private String hostName;
	
	public QueryUnit(QueryUnitId id) {
		this.id = id;
		fragments = new ArrayList<Fragment>();
	}
	
	public void setLogicalPlan(LogicalNode plan) {
    Preconditions.checkArgument(plan.getType() == ExprType.STORE);
    
	  this.plan = plan;
	  store = (CreateTableNode) plan;
	  LogicalNode node = plan;
	  ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
	  s.add(node);
	  int i = 0;
	  while (!s.isEmpty()) {
	    node = s.remove(s.size()-1);
	    if (node instanceof UnaryNode) {
	      UnaryNode unary = (UnaryNode) node;
	      s.add(s.size(), unary.getSubNode());
	    } else if (node instanceof BinaryNode) {
	      scan = new ScanNode[2];
	      BinaryNode binary = (BinaryNode) node;
	      s.add(s.size(), binary.getOuterNode());
	      s.add(s.size(), binary.getInnerNode());
	    } else if (node instanceof ScanNode) {
	      if (scan == null) {
	        scan = new ScanNode[1];
	      }
	      scan[i++] = (ScanNode) node;
	    }
	  }
	}
	
	public void setHost(String host) {
		this.hostName = host;
	}
	
	public void addFragment(Fragment fragment) {
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
	
	public List<Fragment> getFragments() {
		return this.fragments;
	}
	
	public LogicalNode getLogicalPlan() {
	  return this.plan;
	}
	
	public QueryUnitId getId() {
		return id;
	}
	
	public String getHost() {
		return this.hostName;
	}
	
	public String getOutputName() {
		return this.store.getTableName();
	}
	
	public Schema getOutputSchema() {
	  return this.store.getOutputSchema();
	}
	
	public CreateTableNode getStoreTableNode() {
	  return this.store;
	}
	
	public ScanNode[] getScanNodes() {
	  return this.scan;
	}
	
	@Override
	public String toString() {
		String str = new String(plan.getType() + " ");
		for (Fragment t : fragments) {
			str += t + " ";
		}
		return str;
	}
	
	public QueryUnit cloneExceptFragments() {
	  QueryUnit clone = new QueryUnit(QueryIdFactory.newQueryUnitId());
	  try {
      clone.setLogicalPlan((LogicalNode) this.plan.clone());
    } catch (CloneNotSupportedException e) {
      e.printStackTrace();
    }
	  clone.setHost(hostName);

	  return clone;
	}
}