package nta.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import nta.zookeeper.ZkClient;
import nta.zookeeper.ZkUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNtaEngineMaster {
	static NtaTestingUtility util;
	static Configuration conf;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		util = new NtaTestingUtility();
		util.startMiniZKCluster(1);
		conf = util.getConfiguration();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		util.shutdownMiniZKCluster();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBecomeMaster() throws Exception {
		util.startMiniNtaEngineCluster(3);		
		Thread.sleep(1000);
		ZkClient zkClient = new ZkClient(conf);
		assertNotNull(zkClient.exists(NConstants.ZNODE_BASE));
		assertNotNull(zkClient.exists(NConstants.ZNODE_MASTER));
		assertNotNull(zkClient.exists(NConstants.ZNODE_LEAFSERVERS));
		assertNotNull(zkClient.exists(NConstants.ZNODE_QUERIES));
		
		byte [] data = ZkUtil.getDataAndWatch(zkClient, NConstants.ZNODE_MASTER); 
		
		NtaEngineMaster master = util.getMiniNtaEngineCluster().getMaster();
		assertEquals(master.getServerName(), new String(data));
		assertEquals(3, master.getOnlineServer().size());
		assertEquals(3, util.getMiniNtaEngineCluster().getLeafServerThreads().size());
				
		util.shutdownMiniNtaEngineCluster();
	}
}
