package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNtaEngineMaster {
	private static NtaTestingUtility util;
	private static Configuration conf;

	@BeforeClass
	public static void setUp() throws Exception {
		util = new NtaTestingUtility();
		util.startMiniZKCluster(1);
		util.startCatalogCluster();
		conf = util.getConfiguration();
	}

	@AfterClass
	public static void tearDown() throws Exception {
	  util.shutdownCatalogCluster();
		util.shutdownMiniZKCluster();
	}

	@Test
	public void testBecomeMaster() throws Exception {	  
	  final int numLeafs = 3;
		util.startMiniNtaEngineCluster(numLeafs);		
		Thread.sleep(1000);
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
				
		util.shutdownMiniNtaEngineCluster();
	}
}
