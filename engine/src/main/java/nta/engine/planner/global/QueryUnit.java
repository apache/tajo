/**
 * 
 */
package nta.engine.planner.global;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nta.catalog.Schema;
import nta.engine.QueryIdFactory;
import nta.engine.QueryUnitId;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.UnaryNode;

import com.google.common.base.Preconditions;

/**
 * @author jihoon
 *
 */
public class QueryUnit {

	private QueryUnitId id;
	private StoreTableNode store = null;
	private LogicalNode plan = null;
	private List<ScanNode> scan;
	private List<Fragment> fragments;
	
	private String hostName;
	private Map<String, List<URI>> fetchMap;
	
	public QueryUnit(QueryUnitId id) {
		this.id = id;
		fragments = new ArrayList<Fragment>();
		scan = new ArrayList<ScanNode>();
    fetchMap = new HashMap<String, List<URI>>();
	}
	
	public void setLogicalPlan(LogicalNode plan) {
    Preconditions.checkArgument(plan.getType() == ExprType.STORE);
    
	  this.plan = plan;
	  store = (StoreTableNode) plan;
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
	      BinaryNode binary = (BinaryNode) node;
	      s.add(s.size(), binary.getOuterNode());
	      s.add(s.size(), binary.getInnerNode());
	    } else if (node instanceof ScanNode) {
	      scan.add((ScanNode)node);
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
	
	public void addFetch(String key, String uri) throws URISyntaxException {
	  this.addFetch(key, new URI(uri));
	}
	
	public void addFetch(String key, URI uri) {
	  List<URI> uris = null;
	  if (fetchMap.containsKey(key)) {
	    uris = fetchMap.get(key);
	  } else {
	    uris = new ArrayList<URI>();
	  }
	  uris.add(uri);
    fetchMap.put(key, uris);
	}
	
	public void addFetches(String key, List<URI> urilist) {
	  List<URI> uris = null;
    if (fetchMap.containsKey(key)) {
      uris = fetchMap.get(key);
    } else {
      uris = new ArrayList<URI>();
    }
    uris.addAll(urilist);
    fetchMap.put(key, uris);
	}
	
	public void setFetches(Map<String, List<URI>> fetches) {
	  this.fetchMap.clear();
	  this.fetchMap.putAll(fetches);
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
	
	public List<URI> getFetchHosts(String tableId) {
	  return fetchMap.get(tableId);
	}
	
	public Collection<List<URI>> getFetches() {
	  return fetchMap.values();
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
	
	public StoreTableNode getStoreTableNode() {
	  return this.store;
	}
	
	public ScanNode[] getScanNodes() {
	  return this.scan.toArray(new ScanNode[scan.size()]);
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