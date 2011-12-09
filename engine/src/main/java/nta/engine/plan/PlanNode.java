package nta.engine.plan;

import nta.catalog.Schema;
import nta.engine.SchemaObject;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public abstract class PlanNode implements SchemaObject {
	private final NodeType type;
	PlanNode leftNode;
	PlanNode rightNode;
	Schema schema;
	
	public PlanNode(NodeType type) {
		this.type = type;
	}
	
	public NodeType getType() {
		return this.type;
	}
	
	public PlanNode getLeftNode() {
		return this.leftNode;
	}
	
	public PlanNode getRightNode() {
		return this.rightNode;
	}
	
	public void setLeftNode(PlanNode node) {
		this.leftNode = node;
	}
	
	public void setRightNode(PlanNode node) {
		this.rightNode = node;
	}
	
	public void setSchema(Schema schema) {
		this.schema = schema;
	}
	
	public Schema getSchema() {
		return this.schema;
	}
}
