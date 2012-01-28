/**
 * 
 */
package nta.engine.cluster;

import nta.engine.NConstants;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkNodeTracker;

/**
 * @author Hyunsik Choi
 *
 */
public class MasterAddressTracker extends ZkNodeTracker {

	public MasterAddressTracker(ZkClient client) {
		super(client, NConstants.ZNODE_MASTER);
	}

	public ServerName getMasterAddress() {
		byte [] data = super.getData();
		return data == null ? null : new ServerName(new String(data));
	}

	public boolean hasMaster() {
		return super.getData() != null;
	}

	public synchronized ServerName waitForMaster(long timeout)
		throws InterruptedException {
		byte [] data = super.blockUntilAvailable();
		return data == null ? null : new ServerName(new String(data));
	}
}
