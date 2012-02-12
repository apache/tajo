/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nta.distexec.DistPlan;
import nta.engine.QueryUnitId;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.plan.global.Annotation;
import nta.engine.planner.logical.LogicalNode;

/**
 * @author jihoon
 *
 */
public class QueryUnit {

	private QueryUnitId id;
	private LogicalNode op;
	private String tableName;
	private String outputName;
	private List<Fragment> fragments;
	private Set<QueryUnit> prevQueries;
	private Set<QueryUnit> nextQueries;
	private Annotation annotaion;
	private DistPlan distPlan;
	
	private String hostName;
	private int port;
	
	public QueryUnit(QueryUnitId id) {
		this.id = id;
		prevQueries = new HashSet<QueryUnit>();
		nextQueries = new HashSet<QueryUnit>();
		fragments = new ArrayList<Fragment>();
	}
	
	public QueryUnit(QueryUnitId id, LogicalNode op) {
		this(id);
		set(op, null);
	}
	
	public QueryUnit(QueryUnitId id, LogicalNode op, Annotation annotation) {
		this(id);
		set(op, annotation);
	}
	
	public void set(LogicalNode op, Annotation annotation) {
		this.op = op;
		this.annotaion = annotation;
	}
	
	public void setOp(LogicalNode op) {
		this.op = op;
	}
	
	public void setAnnotation(Annotation annotation) {
		this.annotaion = annotation;
	}
	
	public void setDistPlan(DistPlan distPlan) {
		this.distPlan = distPlan;
	}
	
	public void setHost(String host, int port) {
		this.hostName = host;
		this.port = port;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void setOutputName(String outputName) {
		this.outputName = outputName;
	}
	
	public void addFragment(Fragment fragment) {
		this.fragments.add(fragment);
	}
	
	public void setFragments(Fragment[] fragments) {
	  this.fragments.clear();
	  for (Fragment frag : fragments) {
	    this.fragments.add(frag);
	  }
	}
	
	public void addPrevQuery(QueryUnit query) {
		prevQueries.add(query);
	}
	
	public void addNextQuery(QueryUnit query) {
		nextQueries.add(query);
	}
	
	public void removePrevQuery(QueryUnit query) {
		prevQueries.remove(query);
	}
	
	public void removeNextQuery(QueryUnit query) {
		nextQueries.remove(query);
	}
	
	public LogicalNode getOp() {
		return this.op;
	}
	
	public List<Fragment> getFragments() {
		return this.fragments;
	}
	
	public Set<QueryUnit> getPrevQueries() {
		return this.prevQueries;
	}
	
	public Set<QueryUnit> getNextQueries() {
		return this.nextQueries;
	}
	
	public Annotation getAnnotation() {
		return this.annotaion;
	}
	
	public QueryUnitId getId() {
		return id;
	}
	
	public DistPlan getDistPlan() {
		return this.distPlan;
	}
	
	public String getHost() {
		return this.hostName;
	}
	
	public int getPort() {
		return this.port;
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	public String getOutputName() {
		return this.outputName;
	}
	
	@Override
	public String toString() {
		String str = new String(op.getType() + " ");
		for (Fragment t : fragments) {
			str += t + " ";
		}
		return str;
	}
}