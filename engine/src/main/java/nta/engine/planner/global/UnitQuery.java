/**
 * 
 */
package nta.engine.planner.global;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nta.distexec.DistPlan;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.plan.global.Annotation;
import nta.engine.planner.logical.LogicalNode;

/**
 * @author jihoon
 *
 */
public class UnitQuery {

	private static int nextId = -1;

	private static int getNextId() {
		return ++nextId;
	}
	
	private int id;
	private LogicalNode op;
	private String tableName;
	private List<Fragment> fragments;
	private Set<UnitQuery> prevQueries;
	private Set<UnitQuery> nextQueries;
	private Annotation annotaion;
	private DistPlan distPlan;
	
	private String hostName;
	private int port;
	
	public UnitQuery() {
		this.id = getNextId();
		prevQueries = new HashSet<UnitQuery>();
		nextQueries = new HashSet<UnitQuery>();
	}
	
	public UnitQuery(LogicalNode op) {
		this();
		set(op, null);
	}
	
	public UnitQuery(LogicalNode op, Annotation annotation) {
		this();
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
	
	public void addFragment(Fragment fragment) {
		this.fragments.add(fragment);
	}
	
	public void addPrevQuery(UnitQuery query) {
		prevQueries.add(query);
	}
	
	public void addNextQuery(UnitQuery query) {
		nextQueries.add(query);
	}
	
	public void removePrevQuery(UnitQuery query) {
		prevQueries.remove(query);
	}
	
	public void removeNextQuery(UnitQuery query) {
		nextQueries.remove(query);
	}
	
	public LogicalNode getOp() {
		return this.op;
	}
	
	public List<Fragment> getFragments() {
		return this.fragments;
	}
	
	public Set<UnitQuery> getPrevQueries() {
		return this.prevQueries;
	}
	
	public Set<UnitQuery> getNextQueries() {
		return this.nextQueries;
	}
	
	public Annotation getAnnotation() {
		return this.annotaion;
	}
	
	public int getId() {
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
	
	@Override
	public String toString() {
		String str = new String(op.getType() + " ");
		for (Fragment t : fragments) {
			str += t + " ";
		}
		return str;
	}
}