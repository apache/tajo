package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNtaEngineMaster {
	private NtaTestingUtility util;
	private Configuration conf;
	
	private final int numLeafs = 3;

	@Before
	public void setUp() throws Exception {
		util = new NtaTestingUtility();
    util.startMiniCluster(numLeafs);
		conf = util.getConfiguration();
	}

	@After
	public void tearDown() throws Exception {
		util.shutdownMiniCluster();
	}

	@Test
	public void testBecomeMaster() throws Exception {	  
		Thread.sleep(3000);
		
		ZkClient zkClient = new ZkClient(conf);
		assertNotNull(zkClient.exists(NConstants.ZNODE_BASE));
		assertNotNull(zkClient.exists(NConstants.ZNODE_MASTER));
		assertNotNull(zkClient.exists(NConstants.ZNODE_LEAFSERVERS));
		assertNotNull(zkClient.exists(NConstants.ZNODE_QUERIES));
		
		byte [] data = ZkUtil.getDataAndWatch(zkClient, NConstants.ZNODE_MASTER); 
		
		NtaEngineMaster master = util.getMiniNtaEngineCluster().getMaster();
		assertEquals(master.getServerName(), new String(data));
		assertEquals(numLeafs, master.getOnlineServer().size());
		assertEquals(numLeafs, util.getMiniNtaEngineCluster().getLeafServerThreads().size());
	}
}
