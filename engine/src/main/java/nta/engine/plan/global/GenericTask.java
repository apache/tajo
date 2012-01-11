package nta.engine.plan.global;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nta.engine.ipc.protocolrecords.Tablet;
import nta.engine.plan.logical.LogicalOp;
import nta.engine.plan.logical.OpType;

public class GenericTask {

	private LogicalOp op;
	private String tableName;
	private List<Tablet> tablets;
	private Set<GenericTask> prevTasks;
	private Set<GenericTask> nextTasks;
	private Annotation annotation;
	
	public GenericTask() {
		tablets = new ArrayList<Tablet>();
		prevTasks = new HashSet<GenericTask>();
		nextTasks = new HashSet<GenericTask>();
	}
	
	public GenericTask(LogicalOp op, Annotation annotation) {
		this();
		setOp(op);
	}
	
	public void setOp(LogicalOp op) {
		this.op = op;
	}
	
	public void setAnnotation(Annotation annotation) {
		this.annotation = annotation;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void addTablet(Tablet t) {
		tablets.add(t);
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
	
	public OpType getType() {
		return this.op.getType();
	}
	
	public Set<GenericTask> getPrevTasks() {
		return this.prevTasks;
	}
	
	public Set<GenericTask> getNextTasks() {
		return this.nextTasks;
	}
	
	public LogicalOp getOp() {
		return this.op;
	}

	public Annotation getAnnotation() {
		return this.annotation;
	}
	
	public List<Tablet> getTablets() {
		return this.tablets;
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	@Override
	public String toString() {
		String str = new String(op.getType() + " " + tableName + " ");
		for (Tablet t : tablets) {
			str += t + " ";
		}
		return str;
	}
}
