package nta.engine.plan.global;

public class GenericTaskTree {
	private GenericTask root;

	public GenericTaskTree() {
		
	}
	
	public GenericTaskTree(GenericTask root) {
		setRoot(root);
	}
	
	public void setRoot(GenericTask root) {
		this.root = root;
	}
	
	public GenericTask getRoot() {
		return root;
	}
}
