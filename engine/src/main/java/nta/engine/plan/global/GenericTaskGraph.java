package nta.engine.plan.global;

public class GenericTaskGraph {
	private GenericTask root;

	public GenericTaskGraph() {
		
	}
	
	public GenericTaskGraph(GenericTask root) {
		setRoot(root);
	}
	
	public void setRoot(GenericTask root) {
		this.root = root;
	}
	
	public GenericTask getRoot() {
		return root;
	}
}
