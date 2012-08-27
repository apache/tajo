package tajo.engine.plan.global;

import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.LogicalNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GenericTask {

	private LogicalNode op;
	private String tableName;
	private List<Fragment> fragments;
	private Set<GenericTask> prevTasks;
	private Set<GenericTask> nextTasks;
	private Annotation annotation;
	
	public GenericTask() {
		fragments = new ArrayList<Fragment>();
		prevTasks = new HashSet<GenericTask>();
		nextTasks = new HashSet<GenericTask>();
	}
	
	public GenericTask(LogicalNode op, Annotation annotation) {
		this();
		setOp(op);
		setAnnotation(annotation);
	}
	
	public void setOp(LogicalNode op) {
		this.op = op;
	}
	
	public void setAnnotation(Annotation annotation) {
		this.annotation = annotation;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void addFragment(Fragment t) {
		fragments.add(t);
	}
	
	public void addPrevTask(GenericTask t) {
		prevTasks.add(t);
	}
	
	public void addNextTask(GenericTask t) {
		nextTasks.add(t);
	}
	
	public void removePrevTask(GenericTask t) {
		prevTasks.remove(t);
	}
	
	public void removeNextTask(GenericTask t) {
		nextTasks.remove(t);
	}
	
	public ExprType getType() {
		return this.op.getType();
	}
	
	public Set<GenericTask> getPrevTasks() {
		return this.prevTasks;
	}
	
	public Set<GenericTask> getNextTasks() {
		return this.nextTasks;
	}
	
	public LogicalNode getOp() {
		return this.op;
	}

	public Annotation getAnnotation() {
		return this.annotation;
	}
	
	public List<Fragment> getFragments() {
		return this.fragments;
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	public boolean hasFragments() {
		return this.fragments.size() > 0;
	}
	
	@Override
	public String toString() {
		String str = new String(op.getType() + " " + tableName + " ");
		for (Fragment t : fragments) {
			str += t + " ";
		}
		return str;
	}
}
