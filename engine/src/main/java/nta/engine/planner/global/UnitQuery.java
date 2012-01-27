/**
 * 
 */
package nta.engine.planner.global;

import java.util.List;

import nta.engine.ipc.protocolrecords.Fragment;

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
	private LocalizedOp op;
	private List<Fragment> fragments;
	
	public UnitQuery() {
		this.id = getNextId();
	}
	
	public UnitQuery(LocalizedOp op) {
		this();
		setOp(op);
	}
	
	public UnitQuery(LocalizedOp op, List<Fragment> fragments) {
		this();
		set(op, fragments);
	}
	
	public void set(LocalizedOp op, List<Fragment> fragments) {
		this.op = op;
		this.fragments = fragments;
	}
	
	public void setOp(LocalizedOp op) {
		this.op = op;
	}
	
	public void setFragments(List<Fragment> fragments) {
		this.fragments = fragments;
	}
	
	public LocalizedOp getOp() {
		return this.op;
	}
	
	public List<Fragment> getFragments() {
		return this.fragments;
	}
	
	public int getId() {
		return id;
	}
}