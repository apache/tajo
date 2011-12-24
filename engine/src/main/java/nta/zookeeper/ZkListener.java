package nta.zookeeper;

public abstract class ZkListener {

	/**
	   * Called when a new node has been created.
	   * @param path full path of the new node
	   */
	  public void nodeCreated(String path) {}

	  /**
	   * Called when a node has been deleted
	   * @param path full path of the deleted node
	   */
	  public void nodeDeleted(String path) {}

	  /**
	   * Called when an existing node has changed data.
	   * @param path full path of the updated node
	   */
	  public void nodeDataChanged(String path) {}

	  /**
	   * Called when an existing node has a child node added or removed.
	   * @param path full path of the node whose children have changed
	   */
	  public void nodeChildrenChanged(String path) {}
}
